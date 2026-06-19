"""
delete_operation.py  -  Hybrid Database Delete Layer
======================================================
Accepts a delete query JSON and removes data from all relevant backends
(PostgreSQL, MongoDB, buffer.json) using metadata_store.json for routing.

Query formats
-------------
Case A — Delete one full record:
{
  "operation": "delete",
  "where": { "customer_id": 12345 }
}

Case B — Delete a specific entity only (one record):
{
  "operation": "delete",
  "entity": "orders",
  "where": { "customer_id": 12345, "order_id": 101 }
}

Case C — Delete multiple full records in one query:
{
  "operation": "delete",
  "where": { "customer_id": [12345, 67890, 11111] }
}

Case D — Delete a field (column) across ALL records:
{
  "operation": "delete",
  "field": "age"
}
  Drops the column / field from every record on every backend.
  No where clause needed — affects the whole dataset.

  operation  always "delete"
  entity     optional — scopes deletion to one entity for a single record
  field      optional — deletes a field across ALL records (overrides where/entity)
  where      required for A/B/C; global_key value can be a single value or a list

Run
---
    python delete_operation.py                          # interactive prompt
    python delete_operation.py '{"operation":"delete","where":{"customer_id":99999}}'
"""

import json
import os
import sys

from hybriddb.ingestion.classification import _main_table_name
from hybriddb.storage.buffer_store import (
    remove_records    as _buf_remove,
    find_records      as _buf_find,
    save_buffer       as _buf_save,
    load_buffer       as _buf_load,
    update_records_func as _buf_update_func,
)
from hybriddb.config import paths
from hybriddb.core import sql_db
from hybriddb.core.clients import get_mongo_client, get_mongo_db

METADATA_FILE = paths.METADATA_FILE
MONGO_URI     = paths.MONGO_URI
MONGO_DB_NAME = paths.MONGO_DB_NAME


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

def _load_metadata() -> dict:
    if not os.path.exists(METADATA_FILE):
        sys.exit(f"[ERROR] {METADATA_FILE} not found — run classification.py first.")
    with open(METADATA_FILE, "r") as f:
        return json.load(f)


def _build_schema_groups(meta: dict) -> dict:
    """
    Walk metadata fields and produce grouped views used by both delete cases.

    Returns:
      sql_tables      {table_name: {"fk": col_or_None}}  all SQL tables
      main_table      name of the SQL main table
      child_tables    [child table names] — must be deleted before main
      mongo_ref_tops  [top-level field names with Mongo.reference]
      mongo_embed_tops [top-level field names with Mongo.embed]
      buffer_fields   [field names explicitly in Buffer]
    """
    global_key = meta["global_key"]
    main_table = _main_table_name(global_key)
    km         = meta.get("key_management", {})

    sql_km        = km.get("SQL", {})
    sql_tables    = {t: {"fk": info.get("foreign_key")} for t, info in sql_km.items()}
    child_tables  = [t for t, info in sql_km.items() if info.get("foreign_key")]

    embed_list  = km.get("Mongo", {}).get("embed", [])
    ref_list    = km.get("Mongo", {}).get("reference", [])

    # Top-level only (no dot) for collection names
    mongo_embed_tops = list(dict.fromkeys(
        f.split(".")[0] for f in embed_list if "." not in f
    ))
    mongo_ref_tops = list(dict.fromkeys(
        f.split(".")[0] for f in ref_list if "." not in f
    ))

    buffer_fields = list(km.get("Buffer", []))

    return {
        "sql_tables":       sql_tables,
        "main_table":       main_table,
        "child_tables":     child_tables,
        "mongo_ref_tops":   mongo_ref_tops,
        "mongo_embed_tops": mongo_embed_tops,
        "buffer_fields":    buffer_fields,
    }


def _resolve_entity(entity: str, meta: dict) -> dict | None:
    """
    Look up an entity name in metadata fields.
    Returns the field metadata dict or None if not found.
    """
    fields = meta["fields"]
    if entity in fields:
        return fields[entity]
    # Try matching by last segment (e.g. "order" -> "orders")
    for fname, fdata in fields.items():
        if fname.split(".")[-1] == entity and fdata.get("level", 1) == 0:
            return fdata
    return None


# ---------------------------------------------------------------------------
# SQL delete
# ---------------------------------------------------------------------------

def delete_sql_full(gk_val, schema: dict, meta: dict) -> dict:
    """
    Case A — delete every SQL row linked to gk_val.
    Deletes child tables first, then main table, inside one transaction.
    Returns {table: rows_deleted}.
    """
    global_key = meta["global_key"]
    conn       = sql_db.connect()
    deleted    = {}

    try:
        cur = conn.cursor()
        # Child tables first (FK references main table)
        for table in schema["child_tables"]:
            cur.execute(
                f"DELETE FROM {table} WHERE {global_key} = %s", [gk_val]
            )
            deleted[table] = cur.rowcount

        # Main table last
        main = schema["main_table"]
        cur.execute(
            f"DELETE FROM {main} WHERE {global_key} = %s", [gk_val]
        )
        deleted[main] = cur.rowcount

        conn.commit()
    except Exception as exc:
        conn.rollback()
        conn.close()
        raise RuntimeError(f"SQL delete failed — rolled back. Reason: {exc}")

    conn.close()
    return deleted


def delete_sql_entity(entity_table: str, where: dict, meta: dict) -> dict:
    """
    Case B — delete rows from one SQL table matching all where conditions.
    Returns {table: rows_deleted}.
    """
    conn = sql_db.connect()
    deleted = {}

    try:
        cur = conn.cursor()
        where_clause = " AND ".join(f"{col} = %s" for col in where)
        params       = list(where.values())
        cur.execute(
            f"DELETE FROM {entity_table} WHERE {where_clause}", params
        )
        deleted[entity_table] = cur.rowcount
        conn.commit()
    except Exception as exc:
        conn.rollback()
        conn.close()
        raise RuntimeError(f"SQL entity delete failed — rolled back. Reason: {exc}")

    conn.close()
    return deleted


# ---------------------------------------------------------------------------
# MongoDB delete
# ---------------------------------------------------------------------------

def _get_mongo_client():
    """Returns (client, db) or (None, None) if unreachable."""
    try:
        client = get_mongo_client()
        client.admin.command("ping")
        return client, get_mongo_db()
    except ImportError:
        print("  [WARN] pymongo not installed — Mongo delete skipped.")
        return None, None
    except Exception:
        print(f"  [WARN] MongoDB not reachable at {MONGO_URI} — Mongo delete skipped.")
        return None, None


def delete_mongo_full(gk_val, schema: dict, meta: dict) -> dict:
    """
    Case A — delete main collection document and all reference collection docs.
    Returns {"main_collection": n, "ref_collection": n, ...}
    """
    client, db = _get_mongo_client()
    if db is None:
        return {}

    global_key      = meta["global_key"]
    main_collection = _main_table_name(global_key)
    result          = {}

    try:
        # Delete embedded document from main collection
        res = db[main_collection].delete_one({global_key: gk_val})
        result[main_collection] = res.deleted_count

        # Delete all reference collection documents
        for ref_col in schema["mongo_ref_tops"]:
            res = db[ref_col].delete_many({global_key: gk_val})
            result[ref_col] = res.deleted_count

    finally:
        pass  # shared client; do not close

    return result


def delete_mongo_entity(entity: str, where: dict, schema: dict,
                        meta: dict, entity_detail: str) -> dict:
    """
    Case B — delete one entity from Mongo.

    Mongo.reference  -> delete_many on the reference collection
    Mongo.embed      -> $unset (no extra where) or $pull (with extra filter)
    Returns {collection_or_field: n}.
    """
    client, db = _get_mongo_client()
    if db is None:
        return {}

    global_key      = meta["global_key"]
    gk_val          = where[global_key]
    main_collection = _main_table_name(global_key)
    extra_where     = {k: v for k, v in where.items() if k != global_key}
    result          = {}

    try:
        if "reference" in entity_detail:
            # Entity lives in its own collection
            col_name   = entity
            mongo_filter = {global_key: gk_val, **extra_where}
            res          = db[col_name].delete_many(mongo_filter)
            result[col_name] = res.deleted_count

        else:
            # Entity is embedded in the main document
            if extra_where:
                # $pull the matching items from the embedded array
                res = db[main_collection].update_one(
                    {global_key: gk_val},
                    {"$pull": {entity: extra_where}}
                )
                result[entity] = res.modified_count
            else:
                # No extra filter — $unset the whole embedded field
                res = db[main_collection].update_one(
                    {global_key: gk_val},
                    {"$unset": {entity: ""}}
                )
                result[entity] = res.modified_count

    finally:
        pass  # shared client; do not close

    return result


# ---------------------------------------------------------------------------
# Buffer delete
# ---------------------------------------------------------------------------

def delete_buffer_full(gk_val, meta: dict) -> int:
    """
    Case A — remove all buffer records where global_key matches.
    Returns number of records removed.
    """
    global_key = meta["global_key"]
    return _buf_remove({global_key: gk_val})


def delete_buffer_entity(entity: str, where: dict, meta: dict) -> int:
    """
    Case B — for matching buffer records, remove a specific entity field
    from either the top-level record body or unknown_top.
    Returns number of records modified.
    """
    global_key  = meta["global_key"]
    gk_val      = where[global_key]
    extra_where = {k: v for k, v in where.items() if k != global_key}

    # Load all records matching the global key
    buf      = _buf_load()
    recs     = buf.get("records", [])
    modified = 0

    for rec in recs:
        if rec.get(global_key) != gk_val:
            continue

        changed = False

        # Remove from top-level body
        if entity in rec:
            if extra_where and isinstance(rec[entity], list):
                rec[entity] = [
                    item for item in rec[entity]
                    if not all(item.get(k) == v for k, v in extra_where.items())
                ]
                if not rec[entity]:
                    del rec[entity]
            else:
                del rec[entity]
            changed = True

        # Remove from unknown_top
        if entity in rec.get("unknown_top", {}):
            del rec["unknown_top"][entity]
            if not rec["unknown_top"]:
                del rec["unknown_top"]
            changed = True

        if changed:
            modified += 1

    _buf_save(buf)
    return modified


# ---------------------------------------------------------------------------
# Case C helpers  -  multi-record delete
# ---------------------------------------------------------------------------

def _delete_multi_records(gk_vals: list, schema: dict, meta: dict,
                          _auto_confirm: bool) -> dict:
    """
    Delete a list of full records, one at a time, returning aggregate results.
    Returns {"sql": {table: total_rows}, "mongo": {col: total_docs},
             "buffer": total_removed, "errors": []}
    """
    global_key = meta["global_key"]
    agg_sql    = {}
    agg_mongo  = {}
    agg_buf    = 0
    errors     = []

    for gk_val in gk_vals:
        try:
            result = delete_sql_full(gk_val, schema, meta)
            for t, n in result.items():
                agg_sql[t] = agg_sql.get(t, 0) + n
        except RuntimeError as err:
            errors.append(str(err))

        try:
            result = delete_mongo_full(gk_val, schema, meta)
            for col, n in result.items():
                agg_mongo[col] = agg_mongo.get(col, 0) + n
        except Exception as err:
            errors.append(f"Mongo error: {err}")

        agg_buf += delete_buffer_full(gk_val, meta)

    return {"sql": agg_sql, "mongo": agg_mongo, "buffer": agg_buf, "errors": errors}


# ---------------------------------------------------------------------------
# Case D helpers  -  field-wide (column) delete across all records
# ---------------------------------------------------------------------------

def _delete_field_sql(fname: str, detail: str, meta: dict) -> dict:
    """
    Drop a column (primitive) or an entire child table (array/object) from SQL.
    Returns {table: rows_affected}.
    """
    ftype = meta["fields"].get(fname, {}).get("type", "string")
    table = detail.split(".", 1)[1]
    result = {}

    conn = sql_db.connect()
    try:
        cur = conn.cursor()
        if ftype in ("object", "array"):
            # Drop the entire child table
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            cnt = cur.fetchone()[0]
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            result[table] = cnt
        else:
            col = fname.split(".")[-1]
            cur.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {col} IS NOT NULL"
            )
            cnt = cur.fetchone()[0]
            cur.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS {col}")
            result[table] = cnt
        conn.commit()
    except Exception as exc:
        conn.rollback()
        print(f"  [WARN] SQL field delete error: {exc}")
    finally:
        conn.close()

    return result


def _delete_field_mongo(fname: str, detail: str, meta: dict) -> dict:
    """
    $unset a field from all Mongo embed documents, or drop a reference collection.
    Returns {collection: docs_affected}.
    """
    global_key = meta["global_key"]
    main_col   = _main_table_name(global_key)
    client, db = None, None

    try:
        client = get_mongo_client()
        client.admin.command("ping")
        db = client[MONGO_DB_NAME]
    except Exception:
        print("  [WARN] MongoDB unreachable — field not deleted from Mongo.")
        if client:
            pass  # shared client; do not close
        return {}

    result = {}
    try:
        if "reference" in detail:
            cnt = db[fname].count_documents({})
            db.drop_collection(fname)
            result[fname] = cnt
        else:
            # embed / document
            col = fname.split(".")[-1] if "." in fname else fname
            res = db[main_col].update_many(
                {col: {"$exists": True}}, {"$unset": {col: ""}}
            )
            result[main_col] = res.modified_count
    finally:
        pass  # shared client; do not close

    return result


def _delete_field_buffer(fname: str, meta: dict) -> int:
    """
    Remove a field from every record in the MongoDB buffer collection.
    Returns number of records modified.
    """
    col = fname.split(".")[-1]

    def _needs_change(rec):
        return (fname in rec or col in rec
                or fname in rec.get("unknown_top", {}))

    def _apply_change(rec):
        for key in (fname, col):
            rec.pop(key, None)
        if fname in rec.get("unknown_top", {}):
            del rec["unknown_top"][fname]
            if not rec.get("unknown_top"):
                rec.pop("unknown_top", None)
        return rec

    return _buf_update_func(_needs_change, _apply_change)


def _execute_field_delete(fname: str, meta: dict,
                          _auto_confirm: bool = False) -> bool:
    """
    Case D — delete a field across ALL records on every backend.
    """
    if fname not in meta["fields"]:
        print(f"\n  [ERROR] Field '{fname}' not found in metadata. Aborting.")
        return False

    fmeta   = meta["fields"][fname]
    backend = fmeta.get("storage_backend", "Buffer")
    detail  = fmeta.get("storage_detail",  "Buffer")

    print("\n" + "=" * 62)
    print(f"  DELETE FIELD  —  '{fname}'  across all records")
    print("=" * 62)
    print(f"  Backend  : {backend}  ({detail})")
    print(f"  Type     : {fmeta.get('type', '?')}")

    if not _auto_confirm and not _confirm_field_delete(fname):
        print("  Aborted.")
        return False

    sql_result   = {}
    mongo_result = {}
    buf_modified = 0
    errors       = []

    if backend == "SQL":
        sql_result = _delete_field_sql(fname, detail, meta)

    elif backend == "Mongo":
        mongo_result = _delete_field_mongo(fname, detail, meta)

    elif backend == "Buffer":
        buf_modified = _delete_field_buffer(fname, meta)

    # Also scrub from buffer regardless (buffer mirrors all data)
    if backend != "Buffer":
        buf_modified = _delete_field_buffer(fname, meta)

    print("\n" + "-" * 62)
    print("  FIELD DELETE COMPLETE")
    print("-" * 62)
    if sql_result:
        for t, n in sql_result.items():
            print(f"  SQL    : {t}  ({n} rows affected)")
    if mongo_result:
        for col, n in mongo_result.items():
            print(f"  Mongo  : {col}  ({n} documents affected)")
    if buf_modified:
        print(f"  Buffer : {buf_modified} records modified")
    if not sql_result and not mongo_result and not buf_modified:
        print("  (no data found to delete)")

    # Nullify field in metadata so it won't be routed again
    meta["fields"][fname]["storage_backend"] = "Buffer"
    meta["fields"][fname]["storage_detail"]  = "Buffer"
    meta["fields"][fname]["occurrence_count"] = 0
    with open(METADATA_FILE, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"  Metadata : '{fname}' occurrence reset, re-routed to Buffer")
    print("=" * 62)
    return True


def _confirm_field_delete(fname: str) -> bool:
    ans = input(
        f"\n  This will permanently remove '{fname}' from EVERY record "
        "on all backends.\n  Confirm? (yes/no): "
    ).strip().lower()
    return ans in ("yes", "y")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def execute_delete(query: dict, meta: dict, _auto_confirm: bool = False) -> bool:
    """
    Returns True if delete completed, False if aborted or errored.
    _auto_confirm=True bypasses the interactive prompt for full-record deletes.
    Used internally by update_operation.py so the update flow controls the UX.

    Handles four cases:
      A  where={gk: value}              — delete one full record
      B  entity=X, where={gk: value}    — delete one entity on one record
      C  where={gk: [v1,v2,...]}         — delete multiple full records
      D  field=X                         — delete a field across ALL records
    """
    global_key = meta["global_key"]
    where      = query.get("where", {})
    entity     = query.get("entity")
    field      = query.get("field")

    # ── Case D: field-wide delete (no where needed) ───────────────────────
    if field is not None:
        success = _execute_field_delete(field, meta, _auto_confirm)
        if success:
            try:
                from reclassify_migrate import check_and_migrate
                meta, _ = check_and_migrate(meta)
            except Exception as exc:
                print(f"  [WARN] Reclassification check failed: {exc}")
        return success

    # ── All other cases require global_key in where ───────────────────────
    if global_key not in where:
        print(f"[ERROR] 'where' must contain the global_key '{global_key}'. Aborting.")
        return False

    gk_raw = where[global_key]
    schema = _build_schema_groups(meta)

    # ── Case C: multi-record delete ───────────────────────────────────────
    if isinstance(gk_raw, list):
        gk_vals = gk_raw
        print("\n" + "=" * 62)
        print(f"  DELETE OPERATION  —  {len(gk_vals)} Records")
        print("=" * 62)
        print(f"  {global_key} in {gk_vals}")

        if not _auto_confirm:
            ans = input(
                f"\n  This will delete ALL data for {len(gk_vals)} records "
                "across all backends.\n  Confirm? (yes/no): "
            ).strip().lower()
            if ans not in ("yes", "y"):
                print("  Aborted.")
                return False

        agg = _delete_multi_records(gk_vals, schema, meta, _auto_confirm=True)

        print("\n" + "-" * 62)
        print("  MULTI-DELETE COMPLETE")
        print("-" * 62)
        if agg["sql"]:
            for t, n in agg["sql"].items():
                print(f"  SQL    : {t}  ({n} total rows deleted)")
        if agg["mongo"]:
            for col, n in agg["mongo"].items():
                print(f"  Mongo  : {col}  ({n} total documents deleted)")
        print(f"  Buffer : {agg['buffer']} record(s) removed")
        if agg["errors"]:
            for e in agg["errors"]:
                print(f"  [ERROR]: {e}")
        print("=" * 62)

        success = len([e for e in agg["errors"] if not e.startswith("Mongo")]) == 0
        if success:
            try:
                from reclassify_migrate import check_and_migrate
                meta, _ = check_and_migrate(meta)
            except Exception as exc:
                print(f"  [WARN] Reclassification check failed: {exc}")
        return success

    gk_val = gk_raw
    is_full_delete = (entity is None)

    print("\n" + "=" * 62)
    if is_full_delete:
        print("  DELETE OPERATION  —  Full Record")
    else:
        print(f"  DELETE OPERATION  —  Entity: {entity}")
    print("=" * 62)
    print(f"  {global_key} = {gk_val}")
    if not is_full_delete:
        print(f"  entity  = {entity}")
        print(f"  where   = {where}")

    # ── Case A: full record delete ─────────────────────────────────────────
    if is_full_delete:
        if _auto_confirm:
            confirm = "yes"
        else:
            confirm = input(
                f"\n  This will delete ALL data for {global_key}={gk_val} "
                "across all backends.\n  Confirm? (yes/no): "
            ).strip().lower()

        if confirm not in ("yes", "y"):
            print("  Aborted.")
            return False

        sql_result   = {}
        mongo_result = {}
        buf_removed  = 0
        errors       = []

        try:
            sql_result = delete_sql_full(gk_val, schema, meta)
        except RuntimeError as err:
            errors.append(str(err))

        try:
            mongo_result = delete_mongo_full(gk_val, schema, meta)
        except Exception as err:
            errors.append(f"Mongo error: {err}")

        buf_removed = delete_buffer_full(gk_val, meta)

        _print_summary(sql_result, mongo_result, buf_removed, errors, meta)
        sql_errors = [e for e in errors if not e.startswith("Mongo")]
        success = len(sql_errors) == 0

        if success:
            try:
                from reclassify_migrate import check_and_migrate
                meta, _ = check_and_migrate(meta)
            except Exception as exc:
                print(f"  [WARN] Reclassification check failed: {exc}")

        return success

    # ── Case B: entity-level delete ───────────────────────────────────────
    else:
        fmeta = _resolve_entity(entity, meta)
        if fmeta is None:
            print(f"\n  [ERROR] Entity '{entity}' not found in metadata. Aborting.")
            return False

        backend = fmeta.get("storage_backend", "Buffer")
        detail  = fmeta.get("storage_detail",  "Buffer")

        print(f"\n  Resolved: {entity} -> {backend} / {detail}")

        sql_result   = {}
        mongo_result = {}
        buf_modified = 0
        errors       = []

        if backend == "SQL":
            table = detail.split(".", 1)[1]
            try:
                sql_result = delete_sql_entity(table, where, meta)
            except RuntimeError as err:
                errors.append(str(err))

        elif backend == "Mongo":
            try:
                mongo_result = delete_mongo_entity(entity, where, schema, meta, detail)
            except Exception as err:
                errors.append(f"Mongo error: {err}")

        elif backend == "Buffer":
            buf_modified = delete_buffer_entity(entity, where, meta)

        else:
            print(f"  [WARN] Unknown backend '{backend}' for entity '{entity}'.")

        _print_summary(sql_result, mongo_result, buf_modified, errors, meta,
                       entity_mode=True)
        sql_errors = [e for e in errors if not e.startswith("Mongo")]
        success = len(sql_errors) == 0

        if success:
            try:
                from reclassify_migrate import check_and_migrate
                meta, _ = check_and_migrate(meta)
            except Exception as exc:
                print(f"  [WARN] Reclassification check failed: {exc}")

        return success


def _print_summary(sql_result: dict, mongo_result: dict, buf_count: int,
                   errors: list, meta: dict, entity_mode: bool = False):
    label = "modified" if entity_mode else "deleted"

    print("\n" + "-" * 62)
    print("  DELETE COMPLETE")
    print("-" * 62)

    # SQL
    if sql_result:
        parts = ", ".join(
            f"{t} ({n} row {label})" for t, n in sql_result.items()
        )
        print(f"  SQL    : {parts}")
    else:
        print("  SQL    : (nothing changed)")

    # Mongo
    main_collection = _main_table_name(meta["global_key"])
    if mongo_result:
        parts = []
        for col, n in mongo_result.items():
            unit = "document" if n <= 1 else "documents"
            parts.append(f"{col} ({n} {unit} {label})")
        print(f"  Mongo  : {', '.join(parts)}")
    else:
        print("  Mongo  : (nothing changed)")

    # Buffer
    unit = "record" if buf_count == 1 else "records"
    if buf_count:
        print(f"  Buffer : {buf_count} {unit} {label}")
    else:
        print("  Buffer : (nothing changed)")

    # Errors / warnings
    if errors:
        print(f"  Errors : {len(errors)}")
        for e in errors:
            print(f"    - {e}")

    print("=" * 62)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _get_query() -> dict:
    if len(sys.argv) > 1:
        raw = " ".join(sys.argv[1:])
    else:
        print("\nHybrid DB - Delete Operation")
        print("Formats:")
        print('  Full record : {"operation":"delete","where":{"customer_id":99999}}')
        print('  Entity only : {"operation":"delete","entity":"orders","where":{"customer_id":99999}}')
        print()
        raw = input("Query> ").strip()
        if not raw:
            sys.exit("No query provided.")

    try:
        query = json.loads(raw)
    except json.JSONDecodeError as e:
        sys.exit(f"[ERROR] Invalid JSON: {e}")

    if query.get("operation") != "delete":
        sys.exit(
            f"[ERROR] This file only handles operation='delete', "
            f"got '{query.get('operation')}'."
        )
    return query


if __name__ == "__main__":
    meta  = _load_metadata()
    query = _get_query()
    execute_delete(query, meta)
