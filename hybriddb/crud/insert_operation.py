"""
insert_operation.py  -  Hybrid Database Insert Layer
======================================================
Accepts an insert query JSON, validates it against metadata, routes
every field to the correct backend, and writes atomically.

Query format
------------
{
  "operation": "insert",
  "data": {
    "customer_id": 12345,
    "name": "Param",
    "profile": {"bio": "Hello"},
    "orders": [{"order_id": 101, "amount": 500}],
    "random_field": "unexpected"
  }
}

Pipeline
--------
  1. Flatten   — objects → dot-notation paths; arrays stay intact
  2. Validate  — type coerce, not_null check, unique check (against live data)
  3. Route     — bucket every field to SQL / Mongo / Buffer
  4. Write     — SQL (transaction), Mongo (upsert/insert), Buffer (append)
  5. Summary   — print what was written and any warnings

Run
---
    python insert_operation.py                          # interactive prompt
    python insert_operation.py '{"operation":"insert","data":{...}}'
"""

import json
import os
import sys
from copy import deepcopy
from collections import defaultdict
from datetime import datetime, timezone

import psycopg

from hybriddb.config import paths
from hybriddb.core import sql_db
from hybriddb.ingestion.classification import _main_table_name
from hybriddb.crud.read_operation import execute_read
from hybriddb.storage.buffer_store import append_record as _buf_append
from hybriddb.core.clients import get_mongo_client, get_mongo_db

METADATA_FILE = paths.METADATA_FILE
SCHEMA_FILE   = paths.SCHEMA_FILE
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


# ---------------------------------------------------------------------------
# Step 1 — Flatten
# ---------------------------------------------------------------------------

def flatten(data: dict, prefix: str = "") -> dict:
    """
    Recursively expand nested objects into dot-notation keys.
    Arrays are kept as-is — their items are NOT flattened further.

    {"profile": {"bio": "x"}, "orders": [...]}
    ->  {"profile.bio": "x", "orders": [...]}
    """
    result = {}
    for key, value in data.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            result.update(flatten(value, full_key))
        else:
            result[full_key] = value
    return result


# ---------------------------------------------------------------------------
# Step 2 — Validation helpers
# ---------------------------------------------------------------------------

def _coerce(value, expected_type: str):
    """Returns (coerced_value, ok). Attempts safe type conversion."""
    if expected_type == "string":
        return str(value), True
    if expected_type == "boolean":
        return (value, True) if isinstance(value, bool) else (None, False)
    if expected_type == "int":
        if isinstance(value, bool):  return None, False
        if isinstance(value, int):   return value, True
        try:    return int(value), True
        except: return None, False
    if expected_type == "float":
        if isinstance(value, bool):         return None, False
        if isinstance(value, (int, float)): return float(value), True
        try:    return float(value), True
        except: return None, False
    if expected_type == "array":
        return (value, True) if isinstance(value, list) else (None, False)
    if expected_type == "object":
        return (value, True) if isinstance(value, dict) else (None, False)
    return value, True


def _check_not_null(flat_data: dict, meta: dict) -> list[str]:
    """
    Return list of field names that are marked not_null in metadata
    but are absent from the input.

    Child fields (level > 0) are only checked when their top-level parent
    is actually present in the input — if the user didn't supply 'addresses'
    at all, we do not demand 'addresses.address_id'.
    """
    missing     = []
    meta_fields = meta["fields"]

    for fname, fmeta in meta_fields.items():
        if not fmeta.get("not_null"):
            continue

        # ── Child field: only validate if its top-level parent was supplied ──
        parent = fmeta.get("parent")
        if parent is not None:
            # Walk up to the top-level ancestor
            top = fname.split(".")[0]
            parent_supplied = (
                top in flat_data
                or any(k.startswith(f"{top}.") for k in flat_data)
            )
            if not parent_supplied:
                continue       # parent absent → skip child not_null check

        ftype = fmeta.get("type", "string")

        if ftype in ("object", "array"):
            present = fname in flat_data or any(
                k.startswith(f"{fname}.") for k in flat_data
            )
        else:
            col     = fname.split(".")[-1]
            present = fname in flat_data or col in flat_data

        if not present:
            missing.append(fname)

    return missing


def _check_unique(flat_data: dict, meta: dict) -> list[tuple[str, str]]:
    """
    For every field marked unique, query the relevant backend.
    Returns list of (field_name, backend_detail) for each violation found.
    Skips Mongo silently if unreachable.
    """
    violations = []
    meta_fields = meta["fields"]
    global_key  = meta["global_key"]

    mongo_client = None
    mongo_db     = None

    def _get_mongo():
        nonlocal mongo_client, mongo_db
        if mongo_client is not None:
            return mongo_db
        try:
            mongo_client = get_mongo_client()
            mongo_client.admin.command("ping")
            mongo_db = mongo_client[MONGO_DB_NAME]
            return mongo_db
        except Exception:
            mongo_client = False   # sentinel: tried and failed
            return None

    sql_conn = sql_db.dict_connect(autocommit=True) if sql_db.health_check() else None

    try:
        for fname, fmeta in meta_fields.items():
            if not fmeta.get("unique"):
                continue

            ftype   = fmeta.get("type", "string")
            backend = fmeta.get("storage_backend", "Buffer")
            detail  = fmeta.get("storage_detail", "")

            # Locate value in flat_data
            col   = fname.split(".")[-1]
            value = flat_data.get(fname, flat_data.get(col))
            if value is None:
                continue

            if backend == "SQL" and sql_conn:
                table = detail.split(".", 1)[1]
                try:
                    with sql_conn.cursor() as cur:
                        cur.execute(
                            f"SELECT COUNT(*) FROM {table} WHERE {col} = %s", [value]
                        )
                        row = cur.fetchone()
                    count = row["count"] if row else 0
                    if count and count > 0:
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
                        count = db[top].count_documents({col: value})
                    else:
                        count = db[main_col].count_documents({fname: value})
                    if count > 0:
                        violations.append((fname, detail))
                except Exception:
                    pass

    finally:
        if sql_conn:
            sql_db.release(sql_conn)
        if mongo_client and mongo_client is not False:
            pass  # shared client; do not close

    return violations


def _collect_schema_unique_paths(fields: dict, parent: str = "") -> set[str]:
    """Collect fully-qualified field paths that are marked unique in schema.json."""
    unique_paths = set()
    for name, definition in fields.items():
        if not isinstance(definition, dict):
            continue
        full = f"{parent}.{name}" if parent else name
        if definition.get("unique", False):
            unique_paths.add(full)
        nested = definition.get("fields")
        if isinstance(nested, dict):
            unique_paths |= _collect_schema_unique_paths(nested, full)
    return unique_paths


def _schema_identifier_fields(meta: dict) -> set[str]:
    """
    Build the set of identifier fields that must be unique for inserts.
    Source of truth is schema + schema-derived primary keys in key_management.
    """
    ids = set()

    global_key = meta.get("global_key")
    if global_key:
        ids.add(global_key)

    if os.path.exists(SCHEMA_FILE):
        try:
            with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
                raw_schema = json.load(f)
            ids |= _collect_schema_unique_paths(
                {k: v for k, v in raw_schema.items() if k != "global_key"},
                "",
            )
        except Exception:
            # If schema cannot be read, fall back to metadata-only identifiers.
            pass

    fields = meta.get("fields", {})
    km_sql = meta.get("key_management", {}).get("SQL", {})
    for table, info in km_sql.items():
        pk = info.get("primary_key")
        if not pk:
            continue
        matched = False
        for fname, fmeta in fields.items():
            if (
                fmeta.get("storage_backend") == "SQL"
                and fmeta.get("storage_detail") == f"SQL.{table}"
                and fname.split(".")[-1] == pk
            ):
                ids.add(fname)
                matched = True
        if not matched:
            ids.add(pk)

    return ids


def _check_schema_identifiers(flat_data: dict, meta: dict) -> list[tuple[str, str]]:
    """
    Check duplicates only for schema-defined unique fields and primary keys.
    """
    identifiers = _schema_identifier_fields(meta)
    patched_meta = deepcopy(meta)
    for fname, fmeta in patched_meta.get("fields", {}).items():
        fmeta["unique"] = fname in identifiers
    return _check_unique(flat_data, patched_meta)


def validate(flat_data: dict, meta: dict) -> tuple[dict, list[str]]:
    """
    Full validation pass. Returns (validated_flat_data, warnings).
    Raises ValueError with a message on not_null or unique violation.
    """
    meta_fields  = meta["fields"]
    validated    = {}
    warnings     = []

    # ── Type coercion for known fields ────────────────────────────────────
    for key, value in flat_data.items():
        col   = key.split(".")[-1]
        fmeta = meta_fields.get(key) or meta_fields.get(col)

        if fmeta is None:
            validated[key] = value          # unknown field; routing will handle it
            continue

        # Type-widened fields have drifted to mixed types and live in Mongo;
        # store whatever arrives, as-is (no coercion, never discarded).
        if fmeta.get("type_widened"):
            validated[key] = value
            continue

        ftype = fmeta.get("type", "string")
        if ftype in ("object", "array"):
            validated[key] = value          # containers pass through unchanged
            continue

        coerced, ok = _coerce(value, ftype)
        if ok:
            validated[key] = coerced
        else:
            warnings.append(
                f"Field '{key}': expected {ftype}, got {type(value).__name__} "
                f"({value!r}) — field discarded."
            )

    # ── not_null check ────────────────────────────────────────────────────
    missing = _check_not_null(validated, meta)
    if missing:
        raise ValueError(
            f"not_null violation — required field(s) missing from input: "
            + ", ".join(missing)
        )

    # ── Unique check ──────────────────────────────────────────────────────
    violations = _check_schema_identifiers(validated, meta)
    if violations:
        details = ", ".join(f"'{f}' ({d})" for f, d in violations)
        raise ValueError(
            f"Record already exists: duplicate unique/primary key value for: {details}"
        )

    return validated, warnings


# ---------------------------------------------------------------------------
# Step 3 — Route
# ---------------------------------------------------------------------------

def _set_nested(d: dict, dotpath: str, value):
    """Write value into a nested dict following a dot-separated path."""
    parts = dotpath.split(".")
    for part in parts[:-1]:
        d = d.setdefault(part, {})
    d[parts[-1]] = value


def route(validated: dict, meta: dict) -> dict:
    """
    Bucket every validated field into the correct backend container.

    Returns:
      sql_scalar      {table -> {col: val}}   scalar/primitive SQL columns
      sql_arrays      {table -> [item_dicts]} array SQL child rows
      mongo_embed_doc nested doc for main collection (embed/document fields)
      mongo_ref_arrays {collection -> [item_dicts]}  reference collection rows
      buffer_known    {field: val}            fields explicitly routed to Buffer
      unknown_top     {field: val}            unknown fields with no metadata
    """
    meta_fields = meta["fields"]

    sql_scalar      : dict[str, dict] = defaultdict(dict)
    sql_arrays      : dict[str, list] = defaultdict(list)
    mongo_embed_flat: dict[str, object] = {}   # dot-paths -> values
    mongo_ref_arrays: dict[str, list] = defaultdict(list)
    buffer_known    : dict = {}
    unknown_top     : dict = {}

    for key, value in validated.items():

        if key not in meta_fields:
            # ── Unknown field ────────────────────────────────────────────
            if "." not in key:
                unknown_top[key] = value
            else:
                parent = key.rsplit(".", 1)[0]
                if parent in meta_fields:
                    p_backend = meta_fields[parent].get("storage_backend", "Buffer")
                    p_detail  = meta_fields[parent].get("storage_detail", "Buffer")
                    if p_backend == "SQL":
                        table = p_detail.split(".", 1)[1]
                        col   = key.split(".")[-1]
                        sql_scalar[table][col] = value
                    elif p_backend == "Mongo":
                        if "reference" not in p_detail:
                            mongo_embed_flat[key] = value
                        else:
                            unknown_top[key] = value
                    else:
                        unknown_top[key] = value
                else:
                    unknown_top[key] = value
            continue

        fmeta   = meta_fields[key]
        backend = fmeta.get("storage_backend", "Buffer")
        detail  = fmeta.get("storage_detail", "Buffer")
        ftype   = fmeta.get("type", "string")

        # ── SQL ───────────────────────────────────────────────────────────
        if backend == "SQL":
            table = detail.split(".", 1)[1]
            if ftype == "array" and isinstance(value, list):
                sql_arrays[table] = value        # child table rows
            elif ftype not in ("object", "array"):
                col = key.split(".")[-1]
                sql_scalar[table][col] = value   # scalar column

        # ── Mongo ─────────────────────────────────────────────────────────
        elif backend == "Mongo":
            if "reference" in detail:
                top = key.split(".")[0]
                if isinstance(value, list):
                    mongo_ref_arrays[top] = value
            else:
                # embed or document — skip object containers, only keep leaves
                if ftype not in ("object",):
                    mongo_embed_flat[key] = value

        # ── Buffer ────────────────────────────────────────────────────────
        elif backend == "Buffer":
            buffer_known[key] = value

    # Reconstruct nested doc from flat dot-paths for Mongo
    mongo_embed_doc: dict = {}
    for dotpath, value in mongo_embed_flat.items():
        _set_nested(mongo_embed_doc, dotpath, value)

    return {
        "sql_scalar":       dict(sql_scalar),
        "sql_arrays":       dict(sql_arrays),
        "mongo_embed_doc":  mongo_embed_doc,
        "mongo_ref_arrays": dict(mongo_ref_arrays),
        "buffer_known":     buffer_known,
        "unknown_top":      unknown_top,
    }


# ---------------------------------------------------------------------------
# Step 4a — SQL insert
# ---------------------------------------------------------------------------

def insert_sql(sql_scalar: dict, sql_arrays: dict, meta: dict, gk_val) -> dict:
    """
    Write SQL data inside a single transaction.
    Parent (main) table inserted first; child tables second.
    Returns {table: rows_inserted}.
    """
    if not sql_scalar and not sql_arrays:
        return {}

    if not sql_db.health_check():
        print("  [WARN] PostgreSQL not reachable — SQL insert skipped.")
        return {}

    global_key = meta["global_key"]
    km_sql     = meta.get("key_management", {}).get("SQL", {})
    main_table = _main_table_name(global_key)

    inserted = {}

    try:
        with sql_db.transaction() as (conn, cur):
            # ── 1. Insert main (parent) table rows ────────────────────────
            for table, row in sql_scalar.items():
                table_km = km_sql.get(table, {})
                if table_km.get("foreign_key"):
                    continue          # child table — handled below
                if not row:
                    continue

                pk = table_km.get("primary_key")
                sql_db.upsert(cur, table, row, conflict_cols=[pk], update=False)
                inserted[table] = inserted.get(table, 0) + 1

            # ── 2. Insert child table rows (from arrays) ──────────────────
            for table, items in sql_arrays.items():
                table_km     = km_sql.get(table, {})
                fk_col       = table_km.get("foreign_key")
                pk_col       = table_km.get("primary_key")
                is_surrogate = table_km.get("surrogate", False)

                for item in items:
                    if not isinstance(item, dict):
                        continue

                    row = dict(item)

                    # Inject FK
                    if fk_col and fk_col not in row:
                        row[fk_col] = gk_val

                    # Drop surrogate PK — Postgres IDENTITY will generate it
                    if is_surrogate and pk_col and pk_col in row:
                        row = {k: v for k, v in row.items() if k != pk_col}

                    sql_db.insert(cur, table, row)
                    inserted[table] = inserted.get(table, 0) + 1

            # ── 3. Also insert scalar rows for child tables (non-array) ───
            for table, row in sql_scalar.items():
                table_km = km_sql.get(table, {})
                if not table_km.get("foreign_key"):
                    continue          # main table already handled
                if not row:
                    continue

                if global_key not in row:
                    row[global_key] = gk_val

                sql_db.insert(cur, table, row)
                inserted[table] = inserted.get(table, 0) + 1

    except psycopg.errors.UniqueViolation:
        raise RuntimeError(
            "Record already exists: duplicate unique/primary key value."
        )
    except psycopg.errors.ForeignKeyViolation as exc:
        raise RuntimeError(f"SQL transaction failed — rolled back. Reason: {exc}")
    except Exception as exc:
        raise RuntimeError(f"SQL transaction failed — rolled back. Reason: {exc}")

    return inserted


# ---------------------------------------------------------------------------
# Step 4b — Mongo insert
# ---------------------------------------------------------------------------

def insert_mongo(mongo_embed_doc: dict, mongo_ref_arrays: dict,
                 meta: dict, gk_val) -> dict:
    """
    Write Mongo data.
    embed/document fields -> upsert into main collection.
    reference arrays      -> insert each item into its own collection.
    Returns {"embed": upserted_count, "reference": {collection: count}}.
    """
    if not mongo_embed_doc and not mongo_ref_arrays:
        return {}

    try:
        from pymongo import MongoClient
    except ImportError:
        print("  [WARN] pymongo not installed — Mongo insert skipped.")
        return {}

    global_key      = meta["global_key"]
    main_collection = _main_table_name(global_key)

    try:
        client = get_mongo_client()
        client.admin.command("ping")
    except Exception:
        print(f"  [WARN] MongoDB not reachable at {MONGO_URI} — Mongo insert skipped.")
        return {}

    db       = client[MONGO_DB_NAME]
    result   = {"embed": 0, "reference": {}}

    try:
        # ── Embed / document fields → upsert into main collection ──────────
        if mongo_embed_doc:
            doc = {global_key: gk_val, **mongo_embed_doc}
            db[main_collection].update_one(
                {global_key: gk_val},
                {"$set": doc},
                upsert=True,
            )
            result["embed"] = 1

        # ── Reference fields → one doc per array item ─────────────────────
        for collection, items in mongo_ref_arrays.items():
            count = 0
            for item in items:
                if not isinstance(item, dict):
                    continue
                ref_doc = {global_key: gk_val, **item}
                db[collection].insert_one(ref_doc)
                count += 1
            result["reference"][collection] = count

    finally:
        pass  # shared client; do not close

    return result


# ---------------------------------------------------------------------------
# Step 4c — Buffer insert
# ---------------------------------------------------------------------------

def insert_buffer(buffer_known: dict, unknown_top: dict,
                  meta: dict, gk_val) -> int:
    """
    Append a record to the MongoDB buffer collection.
    Returns 1 if a record was written, 0 if nothing to write.
    """
    if not buffer_known and not unknown_top:
        return 0

    global_key = meta["global_key"]

    record = {
        global_key:    gk_val,
        **buffer_known,
        "unknown_top": unknown_top,
        "received_at": datetime.now(timezone.utc).isoformat(),
    }

    _buf_append(record)
    return 1


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def execute_insert(query: dict, meta: dict, _skip_validation: bool = False) -> bool:
    """
    Returns True if all writes completed successfully, False if aborted or failed.

    _skip_validation=True  bypasses the validate() call entirely.
    Used by update_operation.py when pre-validation has already been run before
    the delete step, so re-running it would trigger false unique conflicts.
    """
    data = query.get("data", {})
    if not data:
        print("[ERROR] 'data' key is missing or empty in the query.")
        return False

    global_key = meta["global_key"]
    gk_val = data.get(global_key)

    if gk_val is None:
        print(f"[ERROR] Insert requires '{global_key}' in data.")
        return False

    existing = execute_read(
        {
            "operation": "read",
            "fields": ["*"],
            "where": {global_key: gk_val},
        },
        meta,
    )
    if existing:
        print(f"[ABORT] Record already exists for {global_key}={gk_val}.")
        return False

    print("\n" + "=" * 62)
    print("  INSERT OPERATION")
    print("=" * 62)

    # ── Step 1: flatten ───────────────────────────────────────────────────
    flat = flatten(data)
    print(f"\n  Flattened keys  : {list(flat.keys())}")

    # ── Step 2: validate (skipped when called from update pipeline) ───────
    if _skip_validation:
        validated = flat
        warnings  = []
        print("  Validation      : skipped (pre-validated by caller)")
    else:
        print("  Validating...")
        try:
            validated, warnings = validate(flat, meta)
        except ValueError as err:
            print(f"\n  [ABORT] {err}")
            return False

    for w in warnings:
        print(f"  [WARN] {w}")

    # ── Step 3: route ─────────────────────────────────────────────────────
    buckets = route(validated, meta)

    gk_val = validated.get(global_key)

    print("\n  Routing:")
    for table, cols in buckets["sql_scalar"].items():
        print(f"    SQL.{table} (scalar)    -> {list(cols.keys())}")
    for table, items in buckets["sql_arrays"].items():
        print(f"    SQL.{table} (array)     -> {len(items)} item(s)")
    if buckets["mongo_embed_doc"]:
        print(f"    Mongo.embed            -> {list(buckets['mongo_embed_doc'].keys())}")
    for col, items in buckets["mongo_ref_arrays"].items():
        print(f"    Mongo.reference [{col}] -> {len(items)} item(s)")
    if buckets["buffer_known"]:
        print(f"    Buffer (known)         -> {list(buckets['buffer_known'].keys())}")
    if buckets["unknown_top"]:
        print(f"    Buffer (unknown_top)   -> {list(buckets['unknown_top'].keys())}")

    # ── Step 4: write ─────────────────────────────────────────────────────
    sql_result   = {}
    mongo_result = {}
    buf_result   = 0
    errors       = []

    try:
        sql_result = insert_sql(
            buckets["sql_scalar"],
            buckets["sql_arrays"],
            meta,
            gk_val,
        )
    except RuntimeError as err:
        errors.append(str(err))

    try:
        mongo_result = insert_mongo(
            buckets["mongo_embed_doc"],
            buckets["mongo_ref_arrays"],
            meta,
            gk_val,
        )
    except Exception as err:
        errors.append(f"Mongo error: {err}")

    buf_result = insert_buffer(
        buckets["buffer_known"],
        buckets["unknown_top"],
        meta,
        gk_val,
    )

    # ── Step 5: summary ───────────────────────────────────────────────────
    print("\n" + "-" * 62)
    print("  INSERT COMPLETE")
    print("-" * 62)

    if sql_result:
        sql_summary = ", ".join(f"{t} ({n} row{'s' if n!=1 else ''})"
                                for t, n in sql_result.items())
        print(f"  SQL     : {sql_summary}")
    else:
        print("  SQL     : (nothing written)")

    if mongo_result:
        embed_n = mongo_result.get("embed", 0)
        main_col = _main_table_name(global_key)
        if embed_n:
            print(f"  Mongo   : {main_col} ({embed_n} upserted)")
        for col, n in mongo_result.get("reference", {}).items():
            print(f"  Mongo   : {col} ({n} inserted)")
    else:
        print("  Mongo   : (nothing written)")

    unknown_fields = list(buckets["unknown_top"].keys())
    if buf_result:
        uf_str = f" — unknown fields: {unknown_fields}" if unknown_fields else ""
        print(f"  Buffer  : 1 record appended{uf_str}")
    else:
        print("  Buffer  : (nothing written)")

    if warnings:
        print(f"  Warnings: {len(warnings)}")
        for w in warnings:
            print(f"    - {w}")

    if errors:
        print(f"  Errors  : {len(errors)}")
        for e in errors:
            print(f"    - {e}")

    print("=" * 62)
    # SQL errors are considered fatal; Mongo errors are non-fatal warnings
    sql_errors = [e for e in errors if not e.startswith("Mongo")]
    success = len(sql_errors) == 0

    # ── Post-insert: reclassify + migrate if thresholds shifted ──────────
    if success:
        try:
            from reclassify_migrate import check_and_migrate
            meta, _ = check_and_migrate(meta)
        except Exception as exc:
            print(f"  [WARN] Reclassification check failed: {exc}")

    return success


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _get_query() -> dict:
    if len(sys.argv) > 1:
        raw = " ".join(sys.argv[1:])
    else:
        print("\nHybrid DB - Insert Operation")
        print("Enter insert query JSON (or press Enter for example):")
        print('  {"operation":"insert","data":{"customer_id":99999,"name":"Test",'
              '"email":"test@x.com","profile":{"bio":"Hi","city":"NY"},'
              '"orders":[{"order_id":99901,"amount":250.0}],'
              '"reviews":[{"product_id":5,"rating":4}],'
              '"random_field":"unexpected"}}')
        print()
        raw = input("Query> ").strip()
        if not raw:
            raw = json.dumps({
                "operation": "insert",
                "data": {
                    "customer_id": 99999,
                    "name":        "Test User",
                    "email":       "test@example.com",
                    "profile":     {"bio": "Just testing", "city": "Testville"},
                    "orders":      [{"order_id": 99901, "amount": 250.0},
                                    {"order_id": 99902, "amount": 99.5}],
                    "reviews":     [{"product_id": 5, "rating": 4}],
                    "random_field": "unexpected_value",
                }
            })
            print(f"  Using demo query.")

    try:
        query = json.loads(raw)
    except json.JSONDecodeError as e:
        sys.exit(f"[ERROR] Invalid JSON: {e}")

    if query.get("operation") != "insert":
        sys.exit(f"[ERROR] This file only handles operation='insert', "
                 f"got '{query.get('operation')}'.")
    return query


if __name__ == "__main__":
    meta  = _load_metadata()
    query = _get_query()
    execute_insert(query, meta)
