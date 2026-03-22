"""
read_operation.py  -  Hybrid Database Read Layer
==================================================
Accepts a read query JSON, routes every requested field to the correct
backend (SQL / Mongo / Buffer) using metadata_store.json, executes
the minimum necessary queries, and returns a single merged result per
record.

Query format
------------
{
  "operation": "read",
  "fields": ["customer_id", "name", "orders", "profile"],
  "where": {
    "customer_id": 12345
  }
}

  operation  always "read" here; included for future unified interface
  fields     list of field names to return (parent names auto-expand children)
  where      optional filter dict (keys resolved via metadata)

Run
---
    python read_operation.py                          # interactive prompt
    python read_operation.py '{"operation":"read",...}'  # inline JSON arg
"""

import json
import os
import sys
import sqlite3
from collections import defaultdict

from classification import _main_table_name

METADATA_FILE = "metadata_store.json"
from buffer_store import find_records as _buf_find
SQLITE_FILE   = "hybrid_db.db"
MONGO_URI     = "mongodb://localhost:27017"
MONGO_DB_NAME = "hybrid_db"


# ---------------------------------------------------------------------------
# Metadata loading
# ---------------------------------------------------------------------------

def _load_metadata() -> dict:
    if not os.path.exists(METADATA_FILE):
        sys.exit(f"[ERROR] {METADATA_FILE} not found — run classification.py first.")
    with open(METADATA_FILE, "r") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Field resolution
# ---------------------------------------------------------------------------

def _expand_to_children(fname: str, meta_fields: dict) -> list[str]:
    """
    Return fname plus all its descendants (recursively).
    e.g. "orders" -> ["orders", "orders.order_id", "orders.amount"]
    """
    result = [fname]
    for child in meta_fields.get(fname, {}).get("children", []):
        result.extend(_expand_to_children(child, meta_fields))
    return result


def resolve_fields(requested: list[str], meta: dict) -> dict:
    """
    Map every requested field (and its descendants) to:
      sql_tables        : {table_name -> [column, ...]}
      mongo_embed_tops  : [top-level field names stored embedded in main collection]
      mongo_ref_tops    : [top-level field names stored in reference collections]
      buffer_fields     : [field names in buffer]
      not_found         : [field names absent from metadata]

    For SQL  : individual leaf columns are tracked per table.
    For Mongo: only the top-level field name is tracked (Mongo stores full sub-docs).
    """
    meta_fields    = meta["fields"]
    sql_tables     : dict[str, list] = defaultdict(list)
    mongo_embed    : set[str] = set()
    mongo_ref      : set[str] = set()
    buffer_fields  : set[str] = set()
    not_found      : list[str] = []   # kept for API compat, no longer populated

    seen_cols: dict[str, set] = defaultdict(set)   # table -> seen cols (dedup)

    for fname in requested:
        if fname not in meta_fields:
            # Field not in metadata — may exist in MongoDB buffer's unknown_top.
            # Route it to buffer so query_buffer will look inside unknown_top.
            buffer_fields.add(fname)
            continue

        all_names = _expand_to_children(fname, meta_fields)

        for name in all_names:
            if name not in meta_fields:
                continue

            fmeta   = meta_fields[name]
            backend = fmeta.get("storage_backend") or "Buffer"
            detail  = fmeta.get("storage_detail",  "Buffer")
            ftype   = fmeta.get("type", "string")

            if backend == "SQL":
                table = detail.split(".", 1)[1]
                # Object/array containers have no column — only their leaf children do
                if ftype not in ("object", "array"):
                    col = name.split(".")[-1]
                    if col not in seen_cols[table]:
                        seen_cols[table].add(col)
                        sql_tables[table].append(col)

            elif backend == "Mongo":
                # Track only at the top level so we project the right key
                top = name.split(".")[0]
                if detail in ("Mongo.embed", "Mongo.document"):
                    mongo_embed.add(top)
                elif detail == "Mongo.reference":
                    mongo_ref.add(top)

            elif backend == "Buffer":
                buffer_fields.add(name)

    return {
        "sql_tables":       dict(sql_tables),
        "mongo_embed_tops": list(mongo_embed),
        "mongo_ref_tops":   list(mongo_ref),
        "buffer_fields":    list(buffer_fields),
        "not_found":        not_found,
    }


# ---------------------------------------------------------------------------
# WHERE-clause routing
# ---------------------------------------------------------------------------

def route_where(where: dict, meta: dict) -> dict:
    """
    For every key in the where-dict, find which backend owns it.
    Returns:
      {
        "SQL":    {col: val, ...},
        "Mongo":  {field: val, ...},
        "Buffer": {field: val, ...},
      }
    The global_key filter (if present) is replicated to ALL backends so that
    cross-backend joins work without a second round-trip.
    """
    meta_fields = meta["fields"]
    global_key  = meta["global_key"]

    routed: dict[str, dict] = {"SQL": {}, "Mongo": {}, "Buffer": {}}

    for key, val in where.items():
        if key not in meta_fields:
            # Unknown key — may live in unknown_top in the buffer; route to Buffer.
            routed["Buffer"][key] = val
            continue

        backend = meta_fields[key].get("storage_backend", "Buffer")
        # global_key is always replicated everywhere (the universal join key)
        if key == global_key:
            routed["SQL"][key]    = val
            routed["Mongo"][key]  = val
            routed["Buffer"][key] = val
        else:
            routed[backend][key] = val

    return routed


# ---------------------------------------------------------------------------
# SQL backend
# ---------------------------------------------------------------------------

def query_sql(meta: dict, sql_tables: dict[str, list], where_sql: dict) -> dict:
    """
    Query SQLite for the requested columns.
    Queries each table separately (avoids fan-out from 1:many JOINs).
    Child-table rows (1:many) are grouped into lists keyed by global_key.

    Returns: {gk_val -> {"col": val, ...}}  (flat for main, list for child tables)
    """
    if not sql_tables:
        return {}

    if not os.path.exists(SQLITE_FILE):
        print(f"  [WARN] SQLite file '{SQLITE_FILE}' not found.")
        return {}

    global_key = meta["global_key"]
    km         = meta.get("key_management", {}).get("SQL", {})
    main_table = _main_table_name(global_key)

    # Ensure the global_key column is always fetched (needed for merging)
    for table, cols in sql_tables.items():
        if global_key not in cols:
            cols.insert(0, global_key)

    conn    = sqlite3.connect(SQLITE_FILE)
    conn.row_factory = sqlite3.Row
    results : dict[str, dict] = {}

    try:
        for table, cols in sql_tables.items():
            # Only select columns that actually exist in this table
            table_cols = [c["name"] for c in conn.execute(f"PRAGMA table_info({table})").fetchall()]
            select_cols = [c for c in cols if c in table_cols]
            if not select_cols:
                continue
            # Always include global_key for merging
            if global_key not in select_cols:
                select_cols.insert(0, global_key)

            # Build WHERE clause — only apply conditions relevant to this table
            table_km  = km.get(table, {})
            table_own_cols = set(table_km.get("columns", select_cols))
            applicable_where = {
                k: v for k, v in where_sql.items()
                if k in table_own_cols or k == global_key
            }

            col_str   = ", ".join(select_cols)
            where_str = ""
            params    = []
            if applicable_where:
                where_str = " WHERE " + " AND ".join(f"{k} = ?" for k in applicable_where)
                params     = list(applicable_where.values())

            sql = f"SELECT {col_str} FROM {table}{where_str}"
            rows = conn.execute(sql, params).fetchall()

            is_child = (table != main_table)

            for row in rows:
                row_dict = dict(row)
                gk_val   = row_dict.get(global_key)
                if gk_val is None:
                    continue

                if gk_val not in results:
                    results[gk_val] = {}

                if is_child:
                    # Remove global_key from the child row; it becomes the array item
                    item = {k: v for k, v in row_dict.items() if k != global_key}
                    # Group under the logical parent field name
                    parent_field = _sql_table_to_field(table, meta)
                    key_name = parent_field if parent_field else table
                    if key_name not in results[gk_val]:
                        results[gk_val][key_name] = []
                    results[gk_val][key_name].append(item)
                else:
                    results[gk_val].update(row_dict)

    finally:
        conn.close()

    return results


def _sql_table_to_field(table: str, meta: dict) -> str | None:
    """
    Reverse-look-up: given a child SQL table name, find the top-level
    field name in metadata that maps to SQL.<table>.
    e.g. table="orders" -> field "orders"
    """
    for fname, fmeta in meta["fields"].items():
        if (fmeta.get("storage_detail") == f"SQL.{table}"
                and fmeta.get("level", 0) == 0
                and fmeta.get("type") in ("array", "object")):
            return fname
    return table   # fall back to the table name itself


# ---------------------------------------------------------------------------
# MongoDB backend
# ---------------------------------------------------------------------------

def query_mongo(meta: dict, embed_tops: list[str], ref_tops: list[str],
                where_mongo: dict) -> dict:
    """
    Query MongoDB.
      embed  fields -> project from main customers collection
      reference fields -> query each separate collection

    Returns: {gk_val -> {"field": value, ...}}
    """
    if not embed_tops and not ref_tops:
        return {}

    try:
        from pymongo import MongoClient
    except ImportError:
        print("  [WARN] pymongo not installed — Mongo fields skipped.")
        return {}

    global_key      = meta["global_key"]
    main_collection = _main_table_name(global_key)

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.admin.command("ping")
    except Exception:
        print(f"  [WARN] MongoDB not reachable at {MONGO_URI} — Mongo fields skipped.")
        return {}

    db      = client[MONGO_DB_NAME]
    results : dict[str, dict] = {}

    try:
        # ── Embed fields from main collection ────────────────────────────
        if embed_tops:
            projection = {global_key: 1, "_id": 0}
            for top in embed_tops:
                projection[top] = 1

            mongo_filter = {k: v for k, v in where_mongo.items()}
            for doc in db[main_collection].find(mongo_filter, projection):
                gk_val = doc.get(global_key)
                if gk_val is None:
                    continue
                if gk_val not in results:
                    results[gk_val] = {}
                results[gk_val].update({k: v for k, v in doc.items() if k != "_id"})

        # ── Reference fields from their own collections ───────────────────
        for top in ref_tops:
            mongo_filter = {k: v for k, v in where_mongo.items()}
            projection   = {"_id": 0}   # return all stored fields

            for doc in db[top].find(mongo_filter, projection):
                gk_val = doc.get(global_key)
                if gk_val is None:
                    continue
                if gk_val not in results:
                    results[gk_val] = {}
                # Group documents into a list under the field name
                if top not in results[gk_val]:
                    results[gk_val][top] = []
                item = {k: v for k, v in doc.items() if k != global_key}
                results[gk_val][top].append(item)

    finally:
        client.close()

    return results


# ---------------------------------------------------------------------------
# Buffer backend
# ---------------------------------------------------------------------------

def query_buffer(meta: dict, buffer_fields: list[str], where_buf: dict) -> dict:
    """
    Query the MongoDB buffer collection, apply optional where-filter, and
    extract requested fields.
    Buffer records store:
      - normal validated fields at the top level
      - unknown top-level fields inside the "unknown_top" sub-dict
      - "discarded", "received_at" meta-keys (always ignored)

    Returns: {gk_val -> {"field": value, ...}}
    """
    if not buffer_fields:
        return {}

    # Pass any equality filters directly to MongoDB for efficient lookup
    mongo_where = {k: v for k, v in where_buf.items() if not isinstance(v, list)}
    records = _buf_find(mongo_where if mongo_where else None)

    global_key = meta["global_key"]
    results    : dict[str, dict] = {}
    skip_keys  = {"unknown_top", "discarded", "received_at"}

    for rec in records:
        # Apply where filter
        match = True
        for k, v in where_buf.items():
            rec_val = rec.get(k) or rec.get("unknown_top", {}).get(k)
            if rec_val != v:
                match = False
                break
        if not match:
            continue

        gk_val = rec.get(global_key)
        if gk_val is None:
            continue

        if gk_val not in results:
            results[gk_val] = {}

        for fname in buffer_fields:
            # Check top-level first
            if fname in rec and fname not in skip_keys:
                results[gk_val][fname] = rec[fname]
            # Then look inside unknown_top
            elif fname in rec.get("unknown_top", {}):
                results[gk_val][fname] = rec["unknown_top"][fname]

    return results


# ---------------------------------------------------------------------------
# Result merger
# ---------------------------------------------------------------------------

def merge_results(*backend_results) -> list[dict]:
    """
    Merge dicts from all backends.
    Each dict is {gk_val -> {field: value, ...}}.
    Records with the same gk_val are deep-merged (lists are combined).
    """
    merged: dict = {}

    for result in backend_results:
        for gk_val, data in result.items():
            if gk_val not in merged:
                merged[gk_val] = {}
            for k, v in data.items():
                if k in merged[gk_val] and isinstance(merged[gk_val][k], list) and isinstance(v, list):
                    merged[gk_val][k].extend(v)
                else:
                    merged[gk_val][k] = v

    return list(merged.values())


# ---------------------------------------------------------------------------
# Main query entry point
# ---------------------------------------------------------------------------

def execute_read(query: dict, meta: dict) -> list[dict]:
    """
    Full read pipeline:
      1. Resolve + expand fields
      2. Route WHERE conditions
      3. Query each backend
      4. Merge and return
    """
    requested = query.get("fields", [])
    where     = query.get("where", {})

    if not requested:
        print("[WARN] No fields specified in query.")
        return []

    # Expand wildcard "*" to all top-level fields in metadata
    if "*" in requested:
        top_level = [
            fname for fname, fdata in meta["fields"].items()
            if fdata.get("parent") is None
        ]
        requested = top_level

    # ── Step 1: resolve fields to backends ───────────────────────────────
    resolved = resolve_fields(requested, meta)

    print("\n  Field routing:")
    if resolved["sql_tables"]:
        for tbl, cols in resolved["sql_tables"].items():
            print(f"    SQL.{tbl:<20} -> {cols}")
    if resolved["mongo_embed_tops"]:
        print(f"    Mongo.embed          -> {resolved['mongo_embed_tops']}")
    if resolved["mongo_ref_tops"]:
        print(f"    Mongo.reference      -> {resolved['mongo_ref_tops']}")
    if resolved["buffer_fields"]:
        known_buf   = [f for f in resolved["buffer_fields"] if f in meta["fields"]]
        unknown_buf = [f for f in resolved["buffer_fields"] if f not in meta["fields"]]
        if known_buf:
            print(f"    Buffer (classified)  -> {known_buf}")
        if unknown_buf:
            print(f"    Buffer (unknown_top) -> {unknown_buf}  [not in metadata, checking buffer]")

    # ── Step 2: route WHERE conditions ───────────────────────────────────
    where_routes = route_where(where, meta) if where else {"SQL": {}, "Mongo": {}, "Buffer": {}}

    # ── Step 3: query each backend ───────────────────────────────────────
    sql_result  = query_sql(
        meta,
        resolved["sql_tables"],
        where_routes["SQL"],
    )
    mongo_result = query_mongo(
        meta,
        resolved["mongo_embed_tops"],
        resolved["mongo_ref_tops"],
        where_routes["Mongo"],
    )
    buf_result = query_buffer(
        meta,
        resolved["buffer_fields"],
        where_routes["Buffer"],
    )

    # ── Step 4: merge ─────────────────────────────────────────────────────
    return merge_results(sql_result, mongo_result, buf_result)


# ---------------------------------------------------------------------------
# CLI input helpers
# ---------------------------------------------------------------------------

def _get_query() -> dict:
    """
    Accept the query JSON from:
      1. Command-line argument  (sys.argv[1])
      2. Interactive input prompt
    """
    if len(sys.argv) > 1:
        raw = " ".join(sys.argv[1:])
    else:
        print("\nHybrid DB - Read Operation")
        print("Enter query JSON (or press Enter for example query):")
        print('  {"operation":"read","fields":["customer_id","name","orders","profile"],"where":{"customer_id":1}}')
        print()
        raw = input("Query> ").strip()
        if not raw:
            # Default demo query
            raw = json.dumps({
                "operation": "read",
                "fields": ["customer_id", "name", "email", "age",
                           "orders", "profile", "preferences", "reviews"],
                "where": {}
            })
            print(f"  Using demo query: {raw}")

    try:
        query = json.loads(raw)
    except json.JSONDecodeError as e:
        sys.exit(f"[ERROR] Invalid JSON: {e}")

    if query.get("operation") != "read":
        sys.exit(f"[ERROR] This file only handles operation='read', got '{query.get('operation')}'.")

    return query


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    meta  = _load_metadata()
    query = _get_query()

    print("\n" + "=" * 62)
    print("  READ OPERATION")
    print("=" * 62)
    print(f"  Fields   : {query.get('fields')}")
    print(f"  Where    : {query.get('where', {})}")

    records = execute_read(query, meta)

    print(f"\n  {len(records)} record(s) returned")
    print("=" * 62)

    if records:
        print(json.dumps(records, indent=2, default=str))
    else:
        print("  (no matching records)")
