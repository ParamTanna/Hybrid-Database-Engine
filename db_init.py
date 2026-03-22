"""
db_init.py  -  Database Initialisation
========================================
Reads metadata_store.json and creates the physical databases:

  SQL    ->  hybrid_db.sqlite   (SQLite file, tables derived from metadata)
  Mongo  ->  localhost:27017    database: hybrid_db

Everything is driven purely from metadata - no table names, column names,
or types are hardcoded. If the schema changes and classification is re-run,
re-running this file recreates the databases to match.

Run:
    python db_init.py
"""

import json
import os
import sys
import sqlite3

METADATA_FILE = "metadata_store.json"
from buffer_store import (
    staging_load           as _stg_load,
    flush_staging_to_mongo as _flush_staging,
)
SQLITE_FILE   = "hybrid_db.db"
MONGO_URI     = "mongodb://localhost:27017"
MONGO_DB_NAME = "hybrid_db"

# Map schema types -> SQLite column types
SQL_TYPE_MAP = {
    "int":     "INTEGER",
    "float":   "REAL",
    "string":  "TEXT",
    "boolean": "INTEGER",   # SQLite has no native boolean
    "unknown": "TEXT",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_metadata() -> dict:
    if not os.path.exists(METADATA_FILE):
        sys.exit(f"[ERROR] {METADATA_FILE} not found - run classification.py first.")
    with open(METADATA_FILE, "r") as f:
        return json.load(f)


def _get_col_type(col_name: str, fields: dict, global_key: str) -> str:
    """
    Look up the SQLite type for a column by searching the metadata fields.
    Falls back to TEXT for any unknown column.
    """
    # Direct match
    if col_name in fields:
        return SQL_TYPE_MAP.get(fields[col_name]["type"], "TEXT")

    # Dot-notation child match  e.g. col_name="order_id" -> "orders.order_id"
    for fname, fdata in fields.items():
        if fname.split(".")[-1] == col_name:
            return SQL_TYPE_MAP.get(fdata["type"], "TEXT")

    return "TEXT"


# ---------------------------------------------------------------------------
# SQL setup
# ---------------------------------------------------------------------------

def setup_sqlite(meta: dict):
    """
    Create (or recreate) all SQL tables in hybrid_db.sqlite.
    Table names, columns, PKs and FKs are all read from metadata.
    """
    sql_tables = meta.get("key_management", {}).get("SQL", {})
    fields     = meta["fields"]
    global_key = meta["global_key"]

    if not sql_tables:
        print("[SQL] No SQL tables found in metadata - skipping SQLite setup.")
        return

    # Remove old DB so tables are always fresh after re-classification
    if os.path.exists(SQLITE_FILE):
        os.remove(SQLITE_FILE)

    conn = sqlite3.connect(SQLITE_FILE)
    conn.execute("PRAGMA foreign_keys = ON")

    print("\n" + "=" * 60)
    print("  SQL  -  SQLite Setup")
    print("=" * 60)

    for table_name, table_info in sql_tables.items():
        columns    = table_info["columns"]
        pk_col     = table_info["primary_key"]
        fk_col     = table_info["foreign_key"]
        main_table = [t for t, i in sql_tables.items() if i["primary_key"] is not None]
        main_table = main_table[0] if main_table else None

        is_surrogate = table_info.get("surrogate", False)
        col_defs     = []

        # Surrogate PK: add as first column, AUTOINCREMENT, not from data
        if is_surrogate:
            col_defs.append(f"    {pk_col} INTEGER PRIMARY KEY AUTOINCREMENT")

        for col in columns:
            if is_surrogate and col == pk_col:
                continue          # already added above
            sql_type    = _get_col_type(col, fields, global_key)
            constraints = ""
            if not is_surrogate and col == pk_col:
                constraints = " PRIMARY KEY"
            col_defs.append(f"    {col} {sql_type}{constraints}")

        # Foreign key constraint
        if fk_col and main_table:
            col_defs.append(
                f"    FOREIGN KEY ({fk_col}) REFERENCES {main_table}({fk_col})"
            )

        ddl = (
            f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
            + ",\n".join(col_defs)
            + "\n);"
        )

        conn.execute(ddl)
        conn.commit()

        print(f"\n  Table: {table_name}")
        print(f"    PK  : {pk_col}")
        print(f"    FK  : {fk_col}")
        print(f"    Cols: {columns}")
        print(f"    DDL :")
        for line in ddl.splitlines():
            print(f"          {line}")

    # ── Create indexes after all tables exist ─────────────────────────────
    print("\n  Indexes:")
    for table_name, table_info in sql_tables.items():
        for idx in table_info.get("indexes", []):
            col       = idx["column"]
            unique_kw = "UNIQUE " if idx["unique"] else ""
            idx_name  = f"idx_{table_name}_{col}"
            idx_sql   = (
                f"CREATE {unique_kw}INDEX IF NOT EXISTS {idx_name} "
                f"ON {table_name}({col})"
            )
            conn.execute(idx_sql)
            kind = "UNIQUE" if idx["unique"] else "plain"
            print(f"    {idx_name}  ({kind})")

    conn.commit()
    conn.close()
    print(f"\n  SQLite DB created -> {SQLITE_FILE}")
    print("=" * 60)
    return sql_tables


# ---------------------------------------------------------------------------
# MongoDB setup
# ---------------------------------------------------------------------------

def setup_mongo(meta: dict):
    """
    Connect to MongoDB, create the hybrid_db database and its collections.
    Collection strategy is driven by metadata key_management:
      - embed fields     -> main customer collection  (one document per record)
      - reference fields -> one collection each
    """
    try:
        from pymongo import MongoClient
        from pymongo.errors import ConnectionFailure
    except ImportError:
        sys.exit("[ERROR] pymongo not installed. Run: pip install pymongo")

    mongo_km   = meta.get("key_management", {}).get("Mongo", {})
    global_key = meta["global_key"]

    embed_fields = mongo_km.get("embed", [])
    ref_fields   = mongo_km.get("reference", [])

    print("\n" + "=" * 60)
    print("  MONGO  -  MongoDB Setup")
    print("=" * 60)
    print(f"  URI      : {MONGO_URI}")
    print(f"  Database : {MONGO_DB_NAME}")

    # Derive names before attempting connection so they're always available
    from classification import _main_table_name
    main_collection = _main_table_name(global_key)
    ref_top         = [f for f in ref_fields if "." not in f]

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
    except Exception:
        print(f"\n  [ERROR] MongoDB is not reachable at {MONGO_URI}")
        print("          Start MongoDB then re-run this script.")
        return main_collection, ref_top

    db = client[MONGO_DB_NAME]

    # Ensure main collection exists with a unique index on the global_key
    db[main_collection].create_index(global_key, unique=True)
    print(f"\n  Collection: {main_collection}")
    print(f"    Unique index on: {global_key}")
    print(f"    Embedded fields: {[f for f in embed_fields if '.' not in f]}")

    for ref_entity in ref_top:
        db[ref_entity].create_index(global_key)
        print(f"\n  Collection: {ref_entity}")
        print(f"    Index on: {global_key}  (FK reference)")

    if not ref_top:
        print("\n  No reference collections needed.")

    client.close()
    print(f"\n  MongoDB setup complete on {MONGO_URI}/{MONGO_DB_NAME}")
    print("=" * 60)
    return main_collection, ref_top


# ---------------------------------------------------------------------------
# Load buffer and insert data
# ---------------------------------------------------------------------------

def _load_buffer() -> list:
    """Load all records from the staging buffer (buffer.json)."""
    staging = _stg_load()
    records = staging.get("records", [])
    if not records:
        print("[WARN] Staging buffer (buffer.json) is empty — databases will be empty.")
    return records


def insert_sql(meta: dict, sql_tables: dict, records: list):
    """
    Insert buffer records into SQLite tables.
    For each record, routes each field to its correct table using metadata.
    """
    if not records:
        return

    global_key = meta["global_key"]
    fields     = meta["fields"]
    conn       = sqlite3.connect(SQLITE_FILE)
    conn.execute("PRAGMA foreign_keys = ON")

    # Build a lookup: col_name -> table for O(1) routing.
    # Skip FK columns in child tables so the global_key always maps to the
    # main (PK) table and is not overwritten by child table entries.
    col_to_table = {}
    for table_name, table_info in sql_tables.items():
        fk_col = table_info.get("foreign_key")
        for col in table_info["columns"]:
            if col == fk_col:
                continue          # FK is added automatically during array insert
            col_to_table[col] = table_name

    inserted = {t: 0 for t in sql_tables}

    for record in records:
        row_data:   dict[str, dict]   = {t: {} for t in sql_tables}
        array_rows: dict[str, list]   = {t: [] for t in sql_tables}

        for key, value in record.items():
            if key in ("unknown_top", "discarded", "received_at"):
                continue

            fmeta = fields.get(key)
            if not fmeta or fmeta.get("storage_backend") != "SQL":
                continue

            field_type = fmeta.get("type")

            if field_type in ("object", "array"):
                if field_type == "array" and isinstance(value, list):
                    # Collect child rows; insert AFTER the parent row
                    table  = fmeta["storage_detail"].split(".", 1)[1]
                    fk     = sql_tables[table]["foreign_key"]
                    fk_val = record.get(global_key)
                    for item in value:
                        if not isinstance(item, dict):
                            continue
                        row = {fk: fk_val} if fk else {}
                        row.update(item)
                        array_rows[table].append(row)
                continue

            col   = key.split(".")[-1]
            table = col_to_table.get(col)
            if table:
                row_data[table][col] = value

        # Step 1: insert parent (main) table rows first so FK is satisfied
        for table, row in row_data.items():
            if not row or sql_tables[table].get("foreign_key"):
                continue
            cols         = list(row.keys())
            placeholders = ", ".join(["?"] * len(cols))
            sql = (f"INSERT OR IGNORE INTO {table} "
                   f"({', '.join(cols)}) VALUES ({placeholders})")
            try:
                conn.execute(sql, list(row.values()))
                inserted[table] += 1
            except Exception:
                pass

        # Step 2: insert child (array) rows after parent exists
        for table, rows in array_rows.items():
            pk_col       = sql_tables[table].get("primary_key")
            is_surrogate = sql_tables[table].get("surrogate", False)
            for row in rows:
                if not row:
                    continue
                # Exclude the surrogate PK column - SQLite assigns it automatically
                if is_surrogate and pk_col in row:
                    row = {k: v for k, v in row.items() if k != pk_col}
                cols         = list(row.keys())
                placeholders = ", ".join(["?"] * len(cols))
                sql = (f"INSERT INTO {table} "
                       f"({', '.join(cols)}) VALUES ({placeholders})")
                try:
                    conn.execute(sql, list(row.values()))
                    inserted[table] += 1
                except Exception:
                    pass

    conn.commit()
    conn.close()

    print("\n" + "=" * 60)
    print("  SQL  -  Data Inserted")
    print("=" * 60)
    for table, count in inserted.items():
        print(f"  {table:<20}  {count} rows inserted")
    print("=" * 60)


def insert_mongo(meta: dict, main_collection: str, ref_top: list, records: list):
    """
    Insert buffer records into MongoDB.

    Embed  fields  -> merged into one document per customer in main_collection.
    Reference fields -> each array item becomes its own document in a separate
                        collection (e.g. reviews), with global_key as the FK link.
    """
    if not records:
        return

    try:
        from pymongo import MongoClient
    except ImportError:
        return

    global_key = meta["global_key"]
    fields     = meta["fields"]

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
    except Exception:
        print("[WARN] MongoDB not reachable - skipping Mongo insert.")
        return

    db = client[MONGO_DB_NAME]

    main_inserted = 0
    ref_counts: dict[str, int] = {name: 0 for name in ref_top}

    for record in records:
        gk_val   = record.get(global_key)
        main_doc = {}

        for key, value in record.items():
            if key in ("unknown_top", "discarded", "received_at"):
                continue

            fmeta = fields.get(key)
            if not fmeta:
                continue

            backend = fmeta.get("storage_backend")
            detail  = fmeta.get("storage_detail", "")
            level   = fmeta.get("level", 0)

            if backend != "Mongo" or level != 0:
                continue

            if detail in ("Mongo.embed", "Mongo.document"):
                # Goes directly into the main customer document
                main_doc[key] = value

            elif detail == "Mongo.reference":
                # Each item in the array -> its own document in the ref collection
                if not isinstance(value, list):
                    continue
                ref_col = db[key]           # collection name == field name (e.g. "reviews")
                for item in value:
                    if not isinstance(item, dict):
                        continue
                    ref_doc = {global_key: gk_val, **item}
                    try:
                        ref_col.insert_one(ref_doc)
                        ref_counts[key] = ref_counts.get(key, 0) + 1
                    except Exception:
                        pass

        # Write the embed doc into main collection (upsert so duplicates merge)
        if main_doc:
            if gk_val is not None:
                main_doc[global_key] = gk_val
            try:
                db[main_collection].update_one(
                    {global_key: gk_val},
                    {"$set": main_doc},
                    upsert=True
                )
                main_inserted += 1
            except Exception:
                pass

    client.close()

    print("\n" + "=" * 60)
    print("  MONGO  -  Data Inserted")
    print("=" * 60)
    print(f"  {main_collection:<20}  {main_inserted} documents upserted")
    for col_name, count in ref_counts.items():
        print(f"  {col_name:<20}  {count} documents inserted")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    meta = _load_metadata()

    if not meta.get("key_management"):
        sys.exit("[ERROR] key_management missing - run classification.py first.")

    sql_tables                 = setup_sqlite(meta)
    main_collection, ref_top   = setup_mongo(meta)

    records = _load_buffer()
    print(f"\n  Loading {len(records)} records from staging buffer (buffer.json) ...")

    insert_sql(meta, sql_tables, records)
    insert_mongo(meta, main_collection, ref_top, records)

    # Flush buffer-classified + unknown fields to MongoDB buffer, clear buffer.json
    print("\n  Flushing staging buffer -> MongoDB persistent buffer ...")
    _flush_staging(meta)

    print("\n  Databases ready.\n")
