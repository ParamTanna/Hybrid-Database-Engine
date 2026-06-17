"""
update_operation.py  -  Hybrid Database Update Layer
======================================================
Strategy: UPDATE = DELETE (old) + INSERT (merged)
No direct SQL UPDATE, Mongo $set, or buffer patch logic lives here.
All writes go exclusively through execute_delete and execute_insert.

Query formats
-------------
Case 1 — Update fields on the full record:
{
  "operation": "update",
  "where": { "customer_id": 12345 },
  "data": { "name": "New Name", "profile": { "bio": "Updated bio" } }
}

Case 2 — Update a specific entity (scoped replace):
{
  "operation": "update",
  "entity": "orders",
  "where": { "customer_id": 12345, "order_id": 101 },
  "data": { "amount": 999 }
}

  operation  always "update"
  entity     optional — omit to update the full record
  where      must contain at least global_key
  data       only the fields to change — rest is preserved from old record

Dependencies
------------
  read_operation.py   must expose: execute_read(query, meta) -> list[dict]
  delete_operation.py must expose: execute_delete(query, meta, _auto_confirm)
  insert_operation.py must expose: execute_insert(query, meta)
  classification.py   must expose: _main_table_name(global_key)

Run
---
    python update_operation.py                          # interactive prompt
    python update_operation.py '{"operation":"update","where":{...},"data":{...}}'
"""

import json
import os
import sys

from hybriddb.ingestion.classification import _main_table_name
from hybriddb.crud.read_operation import execute_read
from hybriddb.crud.delete_operation import execute_delete
from hybriddb.crud.insert_operation import (
    execute_insert,
    flatten,
    _coerce,
    _check_not_null,
)
from hybriddb.config import paths
from hybriddb.core import sql_db


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

def _load_metadata() -> dict:
    if not os.path.exists(paths.METADATA_FILE):
        sys.exit(f"[ERROR] {paths.METADATA_FILE} not found — run classification.py first.")
    with open(paths.METADATA_FILE, "r") as f:
        return json.load(f)


def _all_top_level_fields(meta: dict) -> list[str]:
    """Return every level-0 field name — used to read the complete old record."""
    return [
        fname for fname, fdata in meta["fields"].items()
        if fdata.get("level", 0) == 0
    ]


# ---------------------------------------------------------------------------
# Deep merge
# ---------------------------------------------------------------------------

def _deep_merge(old: dict, new: dict) -> dict:
    """
    Merge new into old:
      scalars  — new wins
      dicts    — merge recursively (preserves sub-keys not in new)
      lists    — new wins entirely (no item-level merging for arrays)
    """
    result = dict(old)
    for key, new_val in new.items():
        if (key in result
                and isinstance(result[key], dict)
                and isinstance(new_val, dict)):
            result[key] = _deep_merge(result[key], new_val)
        else:
            result[key] = new_val
    return result


def build_merged_record(old_record: dict, query: dict, meta: dict) -> dict:
    """
    Produce the full merged record that will be deleted then re-inserted.

    Full-record update (no entity):
      Deep-merge data into old_record.  Arrays replace entirely.

    Entity-scoped update (entity given):
      Find the specific item(s) in the entity array matching the extra where
      conditions, merge data into each matched item, rebuild the array.
      The rest of old_record is kept unchanged.
      Insert payload is deliberately scoped to only the entity + global_key
      to avoid re-inserting unchanged rows from other tables.
    """
    global_key  = meta["global_key"]
    gk_val      = query["where"][global_key]
    data        = query.get("data", {})
    entity      = query.get("entity")
    where       = query.get("where", {})
    extra_where = {k: v for k, v in where.items() if k != global_key}

    if entity is None:
        # ── Full record merge ─────────────────────────────────────────────
        merged = _deep_merge(old_record, data)
        merged[global_key] = gk_val           # guarantee global_key present
        return merged

    else:
        # ── Entity-scoped merge ───────────────────────────────────────────
        old_items = old_record.get(entity, [])

        if not isinstance(old_items, list):
            # Scalar / object entity — just overwrite it
            merged = _deep_merge(old_record, {entity: data})
            merged[global_key] = gk_val
            return merged

        if not extra_where:
            # No item-level filter — replace entire array with data
            # data should itself be the new array; fall back to empty list
            new_array = data if isinstance(data, list) else data.get(entity, [])
            return {global_key: gk_val, entity: new_array}

        # Merge only the matched item(s); keep others as-is
        new_items = []
        matched   = 0
        for item in old_items:
            if all(item.get(k) == v for k, v in extra_where.items()):
                new_items.append(_deep_merge(item, data))
                matched += 1
            else:
                new_items.append(item)

        if matched == 0:
            raise ValueError(
                f"No item in '{entity}' matched the where conditions {extra_where}."
            )

        # Return the full updated entity payload so unchanged siblings remain intact.
        merged = dict(old_record)
        merged[global_key] = gk_val
        merged[entity] = new_items
        return merged


# ---------------------------------------------------------------------------
# Pre-validation (runs BEFORE any delete — database is still intact)
# ---------------------------------------------------------------------------

def _pre_validate_unique(flat_merged: dict, meta: dict, gk_val) -> list[tuple[str, str]]:
    """
    Unique-constraint check that excludes the current record itself.

    For SQL : SELECT COUNT(*) FROM table WHERE col=? AND global_key != ?
    For Mongo: count_documents({field: val, global_key: {$ne: gk_val}})

    Returns list of (field_name, backend_detail) for violations.
    """
    violations  = []
    meta_fields = meta["fields"]
    global_key  = meta["global_key"]

    mongo_client = None
    mongo_db     = None

    def _get_mongo():
        nonlocal mongo_client, mongo_db
        if mongo_client is not None:
            return mongo_db
        try:
            from pymongo import MongoClient
            mongo_client = MongoClient(paths.MONGO_URI, serverSelectionTimeoutMS=2000)
            mongo_client.admin.command("ping")
            mongo_db = mongo_client[paths.MONGO_DB_NAME]
            return mongo_db
        except Exception:
            mongo_client = False
            return None

    sql_conn = sql_db.dict_connect(autocommit=True)

    try:
        for fname, fmeta in meta_fields.items():
            if not fmeta.get("unique"):
                continue

            ftype   = fmeta.get("type", "string")
            backend = fmeta.get("storage_backend", "Buffer")
            detail  = fmeta.get("storage_detail", "")

            col   = fname.split(".")[-1]
            value = flat_merged.get(fname, flat_merged.get(col))
            if value is None:
                continue

            if backend == "SQL" and sql_conn:
                table = detail.split(".", 1)[1]
                try:
                    with sql_conn.cursor() as _cur:
                        _cur.execute(
                            f"SELECT COUNT(*) FROM {table} "
                            f"WHERE {col} = %s AND {global_key} != %s",
                            [value, gk_val]
                        )
                        row = _cur.fetchone()
                    if row and list(row.values())[0] > 0:
                        violations.append((fname, detail))
                except Exception:
                    pass

            elif backend == "Mongo":
                db = _get_mongo()
                if db is None:
                    continue
                try:
                    main_col = _main_table_name(global_key)
                    if "reference" in detail:
                        top   = fname.split(".")[0]
                        count = db[top].count_documents(
                            {col: value, global_key: {"$ne": gk_val}}
                        )
                    else:
                        count = db[main_col].count_documents(
                            {fname: value, global_key: {"$ne": gk_val}}
                        )
                    if count > 0:
                        violations.append((fname, detail))
                except Exception:
                    pass

    finally:
        if sql_conn:
            sql_conn.close()
        if mongo_client and mongo_client is not False:
            mongo_client.close()

    return violations


def pre_validate(merged: dict, meta: dict, gk_val,
                 entity: str | None = None) -> tuple[list[str], list[str]]:
    """
    Full pre-validation of the merged record before any write.
    Returns (warnings, errors).  errors is non-empty → caller should abort.

    entity — when set (entity-scoped update), not_null is only checked for
             fields inside that entity; the rest of the record is unchanged
             and doesn't need to satisfy not_null for this operation.
    """
    flat     = flatten(merged)
    warnings = []
    errors   = []

    meta_fields = meta["fields"]

    # ── Type coercion ──────────────────────────────────────────────────────
    for key, value in list(flat.items()):
        col   = key.split(".")[-1]
        fmeta = meta_fields.get(key) or meta_fields.get(col)
        if fmeta is None:
            continue
        ftype = fmeta.get("type", "string")
        if ftype in ("object", "array"):
            continue
        _, ok = _coerce(value, ftype)
        if not ok:
            warnings.append(
                f"Field '{key}': expected {ftype}, got {type(value).__name__} "
                f"({value!r}) — field will be discarded."
            )

    # ── not_null check ─────────────────────────────────────────────────────
    # For entity-scoped updates, only validate not_null on fields that belong
    # to the entity being updated (the rest of the record is untouched).
    if entity is None:
        missing = _check_not_null(flat, meta)
        for m in missing:
            errors.append(f"not_null violation: required field '{m}' is missing.")
    else:
        # Only check not_null for fields whose top-level parent is the entity
        entity_flat = {
            k: v for k, v in flat.items()
            if k == entity or k.startswith(f"{entity}.")
               or k.split(".")[-1] in [
                   f.split(".")[-1]
                   for f in meta_fields
                   if f == entity or f.startswith(f"{entity}.")
               ]
        }
        missing = _check_not_null(entity_flat, meta)
        for m in missing:
            # Only report if it's actually inside this entity
            if m == entity or m.startswith(f"{entity}."):
                errors.append(
                    f"not_null violation: required field '{m}' is missing."
                )

    # ── unique check (exclude self) ─────────────────────────────────────────
    violations = _pre_validate_unique(flat, meta, gk_val)
    for fname, detail in violations:
        errors.append(
            f"unique violation: '{fname}' ({detail}) already exists "
            "in another record."
        )

    return warnings, errors


# ---------------------------------------------------------------------------
# Diff helper (for summary)
# ---------------------------------------------------------------------------

def _changed_fields(old: dict, merged: dict) -> list[str]:
    """Return dot-notation paths where merged differs from old."""
    def _flat(d, prefix=""):
        out = {}
        for k, v in d.items():
            fp = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                out.update(_flat(v, fp))
            else:
                out[fp] = v
        return out

    old_flat    = _flat(old)
    merged_flat = _flat(merged)
    changed     = []
    for k, v in merged_flat.items():
        if old_flat.get(k) != v:
            changed.append(k)
    return changed


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def execute_update(query: dict, meta: dict):
    global_key = meta["global_key"]
    where      = query.get("where", {})
    data       = query.get("data", {})
    entity     = query.get("entity")

    # ── Guard: global_key must be present ────────────────────────────────
    if global_key not in where:
        print(f"[ERROR] 'where' must contain the global_key '{global_key}'. Aborting.")
        return

    gk_val = where[global_key]

    # ── Guard: data must not be empty ────────────────────────────────────
    if not data:
        print("[WARN] Nothing to update — 'data' is empty. Aborting.")
        return

    # ── Guard: entity must exist in metadata ─────────────────────────────
    if entity is not None and entity not in meta["fields"]:
        print(f"[ERROR] Entity '{entity}' not found in metadata. Aborting.")
        return

    print("\n" + "=" * 62)
    print("  UPDATE OPERATION  (strategy: delete + insert)")
    print("=" * 62)
    print(f"  {global_key} = {gk_val}")
    if entity:
        print(f"  entity         = {entity}")
    print(f"  fields in data = {list(flatten(data).keys())}")

    # ── Step 1: read existing record ──────────────────────────────────────
    print("\n  [1/5] Reading existing record...")
    all_fields = _all_top_level_fields(meta)
    read_query = {"operation": "read", "fields": all_fields, "where": {global_key: gk_val}}

    records = execute_read(read_query, meta)

    if not records:
        print(f"  [ABORT] No record found for {global_key}={gk_val} — nothing to update.")
        return

    old_record = records[0]
    print(f"  Found record with fields: {list(old_record.keys())}")

    # ── Step 2: build merged record ───────────────────────────────────────
    print("\n  [2/5] Merging old record with new data...")
    try:
        merged = build_merged_record(old_record, query, meta)
    except ValueError as err:
        print(f"  [ABORT] {err}")
        return

    changed = _changed_fields(old_record, merged)
    print(f"  Fields that will change: {changed if changed else '(none)'}")

    # ── Step 3: pre-validate merged record ───────────────────────────────
    print("\n  [3/5] Validating merged record...")
    warnings, errors = pre_validate(merged, meta, gk_val, entity=entity)

    for w in warnings:
        print(f"  [WARN] {w}")
    if errors:
        for e in errors:
            print(f"  [ABORT] {e}")
        print("  No data has been modified.")
        return

    print("  Validation passed.")

    # ── Step 4: delete old ────────────────────────────────────────────────
    print("\n  [4/5] Deleting old record...")
    print("  " + "-" * 58)
    del_query = {
        "operation": "delete",
        "entity":    entity,
        "where":     where,
    }
    delete_ok = execute_delete(del_query, meta, _auto_confirm=True)
    print("  " + "-" * 58)

    if not delete_ok:
        print("  [ABORT] Delete step failed — data unchanged.")
        return
    print("  Delete complete.")

    # ── Step 5: insert merged ─────────────────────────────────────────────
    print("\n  [5/5] Inserting merged record...")
    print("  " + "-" * 58)
    ins_query = {"operation": "insert", "data": merged}
    insert_ok = execute_insert(ins_query, meta, _skip_validation=True)
    print("  " + "-" * 58)

    if not insert_ok:
        # ── Recovery ─────────────────────────────────────────────────────
        print("  [CRITICAL] INSERT failed after DELETE — attempting recovery...")
        print("  " + "-" * 58)
        rec_query = {"operation": "insert", "data": old_record}
        recovered = execute_insert(rec_query, meta, _skip_validation=True)
        print("  " + "-" * 58)
        if recovered:
            print("  [RECOVERED] Old record successfully re-inserted.")
        else:
            print(
                f"\n  [CRITICAL] Recovery also failed. "
                f"Data for {global_key}={gk_val} may be lost."
                f"\n  Old record dump:\n"
                + json.dumps(old_record, indent=4, default=str)
            )
        return

    print("  Insert complete.")

    # ── Post-update: reclassify + migrate once (delete+insert already ran
    #    their own checks, but update does one authoritative pass here) ────
    try:
        from reclassify_migrate import check_and_migrate
        meta, _ = check_and_migrate(meta)
    except Exception as exc:
        print(f"  [WARN] Reclassification check failed: {exc}")

    # ── Summary ───────────────────────────────────────────────────────────
    _print_summary(query, changed, warnings, meta, gk_val, entity)


def _print_summary(query: dict, changed: list, warnings: list,
                   meta: dict, gk_val, entity):
    global_key      = meta["global_key"]
    main_collection = _main_table_name(global_key)
    km              = meta.get("key_management", {})
    sql_tables      = list(km.get("SQL", {}).keys())
    ref_tops        = [
        f for f in km.get("Mongo", {}).get("reference", []) if "." not in f
    ]

    print("\n" + "-" * 62)
    print("  UPDATE COMPLETE  (strategy: delete + insert)")
    print("-" * 62)
    print(f"  WHERE          : {global_key} = {gk_val}")
    print(f"  Fields updated : {', '.join(changed) if changed else '(none)'}")

    if entity:
        affected_sql   = [t for t in sql_tables if t == entity or
                          meta["fields"].get(entity, {}).get("storage_detail", "").endswith(t)]
        affected_mongo = [entity] if meta["fields"].get(entity, {}).get(
            "storage_backend") == "Mongo" else []
        print(f"  SQL            : {', '.join(affected_sql) if affected_sql else '(not involved)'} (entity replace)")
        print(f"  Mongo          : {', '.join(affected_mongo) if affected_mongo else '(not involved)'} (entity replace)")
        print(f"  Buffer         : (entity fields only)")
    else:
        print(f"  SQL            : {', '.join(sql_tables)} (full replace)")
        print(f"  Mongo          : {main_collection}" +
              (f", {', '.join(ref_tops)}" if ref_tops else "") + " (full replace)")
        print(f"  Buffer         : all matching records replaced")

    if warnings:
        print(f"  Warnings       : {len(warnings)}")
        for w in warnings:
            print(f"    - {w}")

    print("=" * 62)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _get_query() -> dict:
    if len(sys.argv) > 1:
        raw = " ".join(sys.argv[1:])
    else:
        print("\nHybrid DB - Update Operation")
        print("Formats:")
        print('  Full record : {"operation":"update","where":{"customer_id":12345},'
              '"data":{"name":"New Name","profile":{"bio":"Updated"}}}')
        print('  Entity only : {"operation":"update","entity":"orders",'
              '"where":{"customer_id":12345,"order_id":101},"data":{"amount":999}}')
        print()
        raw = input("Query> ").strip()
        if not raw:
            sys.exit("No query provided.")

    try:
        query = json.loads(raw)
    except json.JSONDecodeError as e:
        sys.exit(f"[ERROR] Invalid JSON: {e}")

    if query.get("operation") != "update":
        sys.exit(
            f"[ERROR] This file only handles operation='update', "
            f"got '{query.get('operation')}'."
        )
    return query


if __name__ == "__main__":
    meta  = _load_metadata()
    query = _get_query()
    execute_update(query, meta)
