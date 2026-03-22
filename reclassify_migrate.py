"""
reclassify_migrate.py  -  Post-CRUD Reclassification + Data Migration
======================================================================
After every CRUD operation (insert / update / delete) this module:

  1. Recounts field occurrence_counts from live SQL + Mongo + Buffer
  2. Re-evaluates Phase 3 + 4 classification thresholds
  3. Detects fields whose storage_backend / storage_detail changed
  4. Migrates existing data from the old backend to the new one
  5. Updates key_management and saves metadata_store.json

Migration directions handled
-----------------------------
  SQL primitive  ->  Mongo.embed        (column drops from SQL, upserted to Mongo)
  SQL child table->  Mongo.reference    (SQL table dropped, docs inserted to Mongo)
  Mongo.embed    ->  SQL primitive      (field $unset from Mongo, column added to SQL)
  Mongo.reference->  SQL child table    (collection dropped, rows inserted to SQL)
  SQL / Mongo    ->  Buffer             (data extracted, added to buffer records)
  Buffer         ->  SQL                (buffer fields extracted, inserted into SQL)
  Buffer         ->  Mongo              (buffer fields extracted, upserted to Mongo)

Entry point
-----------
    from reclassify_migrate import check_and_migrate
    meta, migrated = check_and_migrate(meta)   # migrated = list of field names moved
"""

import json
import os
import sqlite3

from classification import (
    _main_table_name,
    _classify_entity,
    _propagate,
    phase5_key_management,
    phase6_storage_map,
    FREQ_RARE,
    FREQ_SQL,
    PRIMITIVE_TYPES,
)

METADATA_FILE = "metadata_store.json"
from buffer_store import (
    load_buffer       as _buf_load,
    save_buffer       as _buf_save,
    find_records      as _buf_find,
)
SQLITE_FILE   = "hybrid_db.db"
MONGO_URI     = "mongodb://localhost:27017"
MONGO_DB_NAME = "hybrid_db"

SQL_TYPE_MAP = {
    "int":     "INTEGER",
    "float":   "REAL",
    "string":  "TEXT",
    "boolean": "INTEGER",
}


# ---------------------------------------------------------------------------
# Mongo helper
# ---------------------------------------------------------------------------

def _get_mongo_db():
    """Returns (client, db) or (None, None) if unreachable."""
    try:
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2_000)
        client.admin.command("ping")
        return client, client[MONGO_DB_NAME]
    except Exception:
        return None, None


# ---------------------------------------------------------------------------
# 1. Recount occurrences from live data
# ---------------------------------------------------------------------------

def _recount_occurrences(meta: dict) -> tuple[int, dict[str, int]]:
    """
    Count how many records contain each field, reading directly from every
    live backend.  Returns (total_records, {fname: occurrence_count}).

    total_records = max distinct global_key count across backends.
    """
    global_key = meta["global_key"]
    fields     = meta["fields"]
    main_table = _main_table_name(global_key)
    km         = meta.get("key_management", {})

    counts: dict[str, int] = {fname: 0 for fname in fields}
    total  = 0

    # ── SQL ──────────────────────────────────────────────────────────────
    if os.path.exists(SQLITE_FILE):
        conn = sqlite3.connect(SQLITE_FILE)
        try:
            row = conn.execute(
                f"SELECT COUNT(*) FROM {main_table}"
            ).fetchone()
            total = max(total, row[0] if row else 0)

            km_sql = km.get("SQL", {})
            for table, tdata in km_sql.items():
                is_child = (table != main_table)
                agg = f"COUNT(DISTINCT {global_key})" if is_child else "COUNT(*)"

                for col in tdata.get("columns", []):
                    if col == global_key and is_child:
                        continue  # FK — skip, not a real data field
                    for fname, fdata in fields.items():
                        if (fdata.get("storage_backend") == "SQL"
                                and fdata.get("storage_detail", "").endswith(f".{table}")
                                and fname.split(".")[-1] == col
                                and fdata["type"] not in ("object", "array")):
                            try:
                                r = conn.execute(
                                    f"SELECT {agg} FROM {table} "
                                    f"WHERE {col} IS NOT NULL"
                                ).fetchone()
                                if r:
                                    counts[fname] = max(counts[fname], r[0])
                            except Exception:
                                pass
        except Exception as exc:
            print(f"  [RECOUNT] SQL error: {exc}")
        finally:
            conn.close()

    # ── Mongo ─────────────────────────────────────────────────────────────
    client, db = _get_mongo_db()
    if db is not None:
        try:
            main_col   = _main_table_name(global_key)
            mongo_total = db[main_col].count_documents({})
            total = max(total, mongo_total)

            for fname, fdata in fields.items():
                backend = fdata.get("storage_backend")
                detail  = fdata.get("storage_detail", "")
                if backend != "Mongo":
                    continue
                if fdata.get("parent") is not None:
                    continue   # children inherit — count only top-level

                if "reference" in detail:
                    pipeline = [
                        {"$group": {"_id": f"${global_key}"}},
                        {"$count": "n"},
                    ]
                    result = list(db[fname].aggregate(pipeline))
                    counts[fname] = max(counts[fname],
                                        result[0]["n"] if result else 0)
                else:
                    cnt = db[main_col].count_documents(
                        {fname: {"$exists": True, "$ne": None}}
                    )
                    counts[fname] = max(counts[fname], cnt)
        except Exception as exc:
            print(f"  [RECOUNT] Mongo error: {exc}")
        finally:
            client.close()

    # ── Buffer (MongoDB) ──────────────────────────────────────────────────
    try:
        records   = _buf_find()
        buf_total = len(records)
        total     = max(total, buf_total)

        for rec in records:
            for fname, fdata in fields.items():
                if fdata.get("parent") is not None:
                    continue
                col = fname.split(".")[-1]
                present = (
                    fname in rec
                    or col in rec
                    or fname in rec.get("unknown_top", {})
                )
                if present:
                    counts[fname] += 1
    except Exception as exc:
        print(f"  [RECOUNT] Buffer error: {exc}")

    # Propagate top-level counts down to children
    for fname, fdata in fields.items():
        parent = fdata.get("parent")
        if parent and parent in counts:
            counts[fname] = max(counts[fname], counts[parent])

    return total, counts


# ---------------------------------------------------------------------------
# 2. Snapshot current backend assignments
# ---------------------------------------------------------------------------

def _snapshot_backends(meta: dict) -> dict[str, tuple[str, str]]:
    """Returns {fname: (storage_backend, storage_detail)} for all fields."""
    return {
        fname: (
            fdata.get("storage_backend") or "Buffer",
            fdata.get("storage_detail")  or "Buffer",
        )
        for fname, fdata in meta["fields"].items()
    }


# ---------------------------------------------------------------------------
# 3. Re-run Phase 3 + 4 silently (no console output)
# ---------------------------------------------------------------------------

def _reclassify_silent(meta: dict) -> dict[str, tuple[str, str]]:
    """
    Recompute frequency and re-classify every top-level field.
    Updates meta["fields"] in-place.
    Returns new snapshot {fname: (backend, detail)}.
    """
    global_key = meta["global_key"]
    fields     = meta["fields"]
    total      = meta["total_records"]

    for fname, fdata in fields.items():
        occ  = fdata.get("occurrence_count", 0)
        freq = (occ / total) if total > 0 else 0.0
        fdata["frequency"] = round(freq, 4)

    top_level = {k: v for k, v in fields.items() if v["parent"] is None}

    for fname, fdata in top_level.items():
        freq              = fdata["frequency"]
        backend, detail   = _classify_entity(fname, fdata, freq, global_key)
        fdata["storage_backend"] = backend
        fdata["storage_detail"]  = detail
        _propagate(fname, backend, detail, fields)

    return _snapshot_backends(meta)


# ---------------------------------------------------------------------------
# 4. Diff: find fields whose backend changed
# ---------------------------------------------------------------------------

def _find_changes(
    old_snap: dict[str, tuple[str, str]],
    new_snap: dict[str, tuple[str, str]],
    meta: dict,
) -> list[tuple[str, str, str]]:
    """
    Return [(fname, old_detail, new_detail)] for TOP-LEVEL fields that changed.
    Children inherit automatically via _propagate so we only handle parents.
    """
    changed = []
    for fname, (new_be, new_det) in new_snap.items():
        old_be, old_det = old_snap.get(fname, ("Buffer", "Buffer"))
        if old_det != new_det and meta["fields"][fname]["parent"] is None:
            changed.append((fname, old_det, new_det))
    return changed


# ---------------------------------------------------------------------------
# 5. Migration helpers
# ---------------------------------------------------------------------------

def _sql_type(fname: str, meta: dict) -> str:
    ftype = meta["fields"].get(fname, {}).get("type", "string")
    return SQL_TYPE_MAP.get(ftype, "TEXT")


def _recreate_sql_table_drop_col(conn: sqlite3.Connection,
                                  table: str, drop_col: str):
    """
    SQLite-safe column removal via table recreation.
    Copies all data to a temp table, drops original, renames.
    """
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    all_cols = [r[1] for r in rows]
    keep_cols = [c for c in all_cols if c != drop_col]
    if not keep_cols:
        return

    col_defs = []
    for r in rows:
        if r[1] == drop_col:
            continue
        notnull = " NOT NULL" if r[3] else ""
        pk      = " PRIMARY KEY" if r[5] else ""
        col_defs.append(f"{r[1]} {r[2]}{notnull}{pk}")

    tmp = f"_tmp_{table}"
    conn.execute(f"CREATE TABLE {tmp} ({', '.join(col_defs)})")
    cols_str = ", ".join(keep_cols)
    conn.execute(f"INSERT INTO {tmp} ({cols_str}) SELECT {cols_str} FROM {table}")
    conn.execute(f"DROP TABLE {table}")
    conn.execute(f"ALTER TABLE {tmp} RENAME TO {table}")


# ── A: SQL primitive field → Mongo.embed ─────────────────────────────────

def _migrate_sql_to_mongo_embed(fname: str, old_table: str, meta: dict):
    global_key = meta["global_key"]
    col        = fname.split(".")[-1]
    main_col   = _main_table_name(global_key)

    client, db = _get_mongo_db()

    # Pull from SQL
    sql_data: list[tuple] = []
    if os.path.exists(SQLITE_FILE):
        conn = sqlite3.connect(SQLITE_FILE)
        try:
            sql_data = conn.execute(
                f"SELECT {global_key}, {col} FROM {old_table} WHERE {col} IS NOT NULL"
            ).fetchall()
        finally:
            conn.close()

    # Write to Mongo
    if db is not None and sql_data:
        for gk_val, val in sql_data:
            db[main_col].update_one(
                {global_key: gk_val}, {"$set": {col: val}}, upsert=True
            )
        client.close()
    elif client:
        client.close()

    # Remove column from SQL
    if os.path.exists(SQLITE_FILE):
        conn = sqlite3.connect(SQLITE_FILE)
        try:
            conn.execute("BEGIN")
            try:
                conn.execute(f"ALTER TABLE {old_table} DROP COLUMN {col}")
            except Exception:
                _recreate_sql_table_drop_col(conn, old_table, col)
            conn.execute("COMMIT")
        except Exception as exc:
            conn.execute("ROLLBACK")
            print(f"  [MIGRATE] SQL col drop failed: {exc}")
        finally:
            conn.close()

    moved = len(sql_data)
    print(f"  [MIGRATE] {fname}: SQL.{old_table} -> Mongo.embed  ({moved} values)")


# ── B: SQL child table → Mongo.reference ─────────────────────────────────

def _migrate_sql_child_to_mongo_ref(fname: str, old_table: str, meta: dict):
    global_key = meta["global_key"]

    # Pull all rows from SQL child table
    rows_data: list[dict] = []
    if os.path.exists(SQLITE_FILE):
        conn = sqlite3.connect(SQLITE_FILE)
        conn.row_factory = sqlite3.Row
        try:
            rows_data = [dict(r) for r in
                         conn.execute(f"SELECT * FROM {old_table}").fetchall()]
        finally:
            conn.close()

    # Insert into Mongo reference collection
    client, db = _get_mongo_db()
    if db is not None and rows_data:
        db[fname].insert_many(rows_data)
        client.close()
    elif client:
        client.close()

    # Drop SQL child table
    if os.path.exists(SQLITE_FILE):
        conn = sqlite3.connect(SQLITE_FILE)
        try:
            conn.execute(f"DROP TABLE IF EXISTS {old_table}")
            conn.commit()
        finally:
            conn.close()

    print(f"  [MIGRATE] {fname}: SQL.{old_table} -> Mongo.reference  "
          f"({len(rows_data)} rows)")


# ── C: Mongo.embed → SQL primitive field ─────────────────────────────────

def _migrate_mongo_embed_to_sql(fname: str, new_table: str, meta: dict):
    global_key = meta["global_key"]
    col        = fname.split(".")[-1]
    sql_type   = _sql_type(fname, meta)
    main_col   = _main_table_name(global_key)

    mongo_data: list[tuple] = []
    client, db = _get_mongo_db()
    if db is not None:
        for doc in db[main_col].find({fname: {"$exists": True, "$ne": None}},
                                      {global_key: 1, fname: 1}):
            mongo_data.append((doc.get(global_key), doc.get(fname)))

    # Add column to SQL table if not present
    if os.path.exists(SQLITE_FILE):
        conn = sqlite3.connect(SQLITE_FILE)
        try:
            existing = [r[1] for r in
                        conn.execute(f"PRAGMA table_info({new_table})").fetchall()]
            if col not in existing:
                conn.execute(
                    f"ALTER TABLE {new_table} ADD COLUMN {col} {sql_type}"
                )
                conn.commit()

            # Populate column
            for gk_val, val in mongo_data:
                conn.execute(
                    f"UPDATE {new_table} SET {col} = ? WHERE {global_key} = ?",
                    [val, gk_val]
                )
            conn.commit()
        except Exception as exc:
            print(f"  [MIGRATE] SQL ADD COLUMN failed: {exc}")
        finally:
            conn.close()

    # $unset from Mongo
    if db is not None:
        db[main_col].update_many({}, {"$unset": {col: ""}})
        client.close()
    elif client:
        client.close()

    print(f"  [MIGRATE] {fname}: Mongo.embed -> SQL.{new_table}  "
          f"({len(mongo_data)} values)")


# ── D: Mongo.reference collection → SQL child table ──────────────────────

def _migrate_mongo_ref_to_sql_child(fname: str, meta: dict):
    global_key = meta["global_key"]
    new_table  = fname   # fname IS the child table name (e.g. "orders")

    client, db = _get_mongo_db()
    mongo_rows: list[dict] = []
    if db is not None:
        mongo_rows = list(db[fname].find({}, {"_id": 0}))

    # Determine columns from metadata children
    km   = meta.get("key_management", {})
    cols = km.get("SQL", {}).get(new_table, {}).get("columns", [])
    pk   = km.get("SQL", {}).get(new_table, {}).get("primary_key")
    fk   = km.get("SQL", {}).get(new_table, {}).get("foreign_key")
    surr = km.get("SQL", {}).get(new_table, {}).get("surrogate", False)

    if os.path.exists(SQLITE_FILE) and cols:
        conn = sqlite3.connect(SQLITE_FILE)
        try:
            # Build CREATE TABLE DDL
            col_defs = []
            for c in cols:
                fmeta = next(
                    (v for k, v in meta["fields"].items()
                     if k.split(".")[-1] == c
                     and v.get("storage_detail", "").endswith(f".{new_table}")),
                    None
                )
                sql_t = SQL_TYPE_MAP.get(
                    fmeta["type"] if fmeta else "string", "TEXT"
                )
                if c == pk and not surr:
                    col_defs.append(f"{c} {sql_t} PRIMARY KEY")
                elif c == pk and surr:
                    col_defs.append(f"{c} INTEGER PRIMARY KEY AUTOINCREMENT")
                elif c == fk:
                    col_defs.append(
                        f"{c} {sql_t}, "
                        f"FOREIGN KEY ({c}) REFERENCES "
                        f"{_main_table_name(global_key)}({c})"
                    )
                else:
                    col_defs.append(f"{c} {sql_t}")

            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {new_table} ({', '.join(col_defs)})"
            )

            # Insert rows
            insert_cols = [c for c in cols if not (surr and c == pk)]
            placeholders = ", ".join("?" * len(insert_cols))
            for row in mongo_rows:
                vals = [row.get(c) for c in insert_cols]
                conn.execute(
                    f"INSERT OR IGNORE INTO {new_table} "
                    f"({', '.join(insert_cols)}) VALUES ({placeholders})",
                    vals
                )
            conn.commit()
        except Exception as exc:
            print(f"  [MIGRATE] SQL child table create failed: {exc}")
        finally:
            conn.close()

    # Drop Mongo collection
    if db is not None:
        db.drop_collection(fname)
        client.close()
    elif client:
        client.close()

    print(f"  [MIGRATE] {fname}: Mongo.reference -> SQL.{new_table}  "
          f"({len(mongo_rows)} rows)")


# ── E: Any backend → Buffer ───────────────────────────────────────────────

def _migrate_to_buffer(fname: str, old_detail: str, meta: dict):
    global_key = meta["global_key"]
    col        = fname.split(".")[-1]
    migrated   = 0

    buf          = _buf_load()
    existing_gks = {rec.get(global_key) for rec in buf["records"]}

    if old_detail.startswith("SQL."):
        table = old_detail.split(".", 1)[1]
        if os.path.exists(SQLITE_FILE):
            conn = sqlite3.connect(SQLITE_FILE)
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute(
                    f"SELECT {global_key}, {col} FROM {table} "
                    f"WHERE {col} IS NOT NULL"
                ).fetchall()
                for row in rows:
                    gk_val = row[global_key]
                    val    = row[col]
                    rec = next((r for r in buf["records"]
                                if r.get(global_key) == gk_val), None)
                    if rec is None:
                        rec = {global_key: gk_val, "unknown_top": {},
                               "discarded": {}, "received_at": "migrated"}
                        buf["records"].append(rec)
                        buf["total_buffered"] += 1
                    rec[fname] = val
                    migrated += 1
            finally:
                conn.close()

            # Remove column from SQL
            conn = sqlite3.connect(SQLITE_FILE)
            try:
                conn.execute("BEGIN")
                try:
                    conn.execute(f"ALTER TABLE {table} DROP COLUMN {col}")
                except Exception:
                    _recreate_sql_table_drop_col(conn, table, col)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
            finally:
                conn.close()

    elif old_detail in ("Mongo.embed", "Mongo.document"):
        client, db = _get_mongo_db()
        if db is not None:
            main_col = _main_table_name(global_key)
            for doc in db[main_col].find(
                {fname: {"$exists": True}}, {global_key: 1, fname: 1}
            ):
                gk_val = doc.get(global_key)
                val    = doc.get(fname)
                rec = next((r for r in buf["records"]
                             if r.get(global_key) == gk_val), None)
                if rec is None:
                    rec = {global_key: gk_val, "unknown_top": {},
                           "discarded": {}, "received_at": "migrated"}
                    buf["records"].append(rec)
                    buf["total_buffered"] += 1
                rec[fname] = val
                migrated += 1
            db[main_col].update_many({}, {"$unset": {fname: ""}})
            client.close()

    elif old_detail == "Mongo.reference":
        client, db = _get_mongo_db()
        if db is not None:
            for doc in db[fname].find({}, {"_id": 0}):
                gk_val = doc.get(global_key)
                rec = next((r for r in buf["records"]
                             if r.get(global_key) == gk_val), None)
                if rec is None:
                    rec = {global_key: gk_val, "unknown_top": {},
                           "discarded": {}, "received_at": "migrated"}
                    buf["records"].append(rec)
                    buf["total_buffered"] += 1
                rec.setdefault(fname, [])
                doc_copy = {k: v for k, v in doc.items() if k != global_key}
                rec[fname].append(doc_copy)
                migrated += 1
            db.drop_collection(fname)
            client.close()

    _buf_save(buf)
    print(f"  [MIGRATE] {fname}: {old_detail} -> Buffer  ({migrated} values)")


# ── F: Buffer → SQL ───────────────────────────────────────────────────────

def _migrate_buffer_to_sql(fname: str, new_table: str, meta: dict):
    global_key = meta["global_key"]
    col        = fname.split(".")[-1]
    sql_type   = _sql_type(fname, meta)
    migrated   = 0

    buf = _buf_load()

    if os.path.exists(SQLITE_FILE):
        conn = sqlite3.connect(SQLITE_FILE)
        try:
            # Add column if needed
            existing = [r[1] for r in
                        conn.execute(f"PRAGMA table_info({new_table})").fetchall()]
            if col not in existing:
                conn.execute(
                    f"ALTER TABLE {new_table} ADD COLUMN {col} {sql_type}"
                )

            for rec in buf["records"]:
                val    = rec.get(fname) or rec.get(col)
                gk_val = rec.get(global_key)
                if val is None or gk_val is None:
                    continue
                conn.execute(
                    f"UPDATE {new_table} SET {col} = ? WHERE {global_key} = ?",
                    [val, gk_val]
                )
                if col in rec:
                    del rec[col]
                if fname in rec:
                    del rec[fname]
                migrated += 1
            conn.commit()
        except Exception as exc:
            print(f"  [MIGRATE] Buffer->SQL error: {exc}")
        finally:
            conn.close()

    _buf_save(buf)
    print(f"  [MIGRATE] {fname}: Buffer -> SQL.{new_table}  ({migrated} values)")


# ── G: Buffer → Mongo ─────────────────────────────────────────────────────

def _migrate_buffer_to_mongo(fname: str, new_detail: str, meta: dict):
    global_key = meta["global_key"]
    main_col   = _main_table_name(global_key)
    migrated   = 0

    buf = _buf_load()

    client, db = _get_mongo_db()
    if db is None:
        if client:
            client.close()
        print(f"  [MIGRATE] Buffer -> Mongo skipped (Mongo unreachable)")
        return

    try:
        for rec in buf["records"]:
            val    = rec.get(fname)
            gk_val = rec.get(global_key)
            if val is None or gk_val is None:
                continue

            if "reference" in new_detail:
                items = val if isinstance(val, list) else [val]
                for item in items:
                    if isinstance(item, dict):
                        item[global_key] = gk_val
                        db[fname].insert_one(item)
                        migrated += 1
            else:
                db[main_col].update_one(
                    {global_key: gk_val},
                    {"$set": {fname: val}},
                    upsert=True
                )
                migrated += 1

            if fname in rec:
                del rec[fname]

    finally:
        client.close()

    _buf_save(buf)
    print(f"  [MIGRATE] {fname}: Buffer -> {new_detail}  ({migrated} values)")


# ---------------------------------------------------------------------------
# 6. Migration dispatcher
# ---------------------------------------------------------------------------

def _migrate_field(fname: str, old_detail: str, new_detail: str, meta: dict):
    """Route to the correct migration function based on source → destination."""
    old_is_sql   = old_detail.startswith("SQL.")
    new_is_sql   = new_detail.startswith("SQL.")
    old_is_mongo = old_detail.startswith("Mongo.")
    new_is_mongo = new_detail.startswith("Mongo.")
    old_is_buf   = old_detail == "Buffer"
    new_is_buf   = new_detail == "Buffer"

    old_table = old_detail.split(".", 1)[1] if old_is_sql else None
    new_table = new_detail.split(".", 1)[1] if new_is_sql else None

    ftype = meta["fields"].get(fname, {}).get("type", "string")

    if old_is_sql and new_is_mongo:
        if ftype in PRIMITIVE_TYPES:
            _migrate_sql_to_mongo_embed(fname, old_table, meta)
        else:
            # Array/object child table → Mongo reference
            _migrate_sql_child_to_mongo_ref(fname, old_table, meta)

    elif old_is_mongo and new_is_sql:
        old_is_ref = "reference" in old_detail
        if ftype in PRIMITIVE_TYPES:
            _migrate_mongo_embed_to_sql(fname, new_table, meta)
        else:
            if old_is_ref:
                _migrate_mongo_ref_to_sql_child(fname, meta)
            else:
                # embed → SQL child: treat same as ref → SQL child
                _migrate_mongo_ref_to_sql_child(fname, meta)

    elif new_is_buf:
        _migrate_to_buffer(fname, old_detail, meta)

    elif old_is_buf and new_is_sql:
        _migrate_buffer_to_sql(fname, new_table, meta)

    elif old_is_buf and new_is_mongo:
        _migrate_buffer_to_mongo(fname, new_detail, meta)

    elif old_is_sql and new_is_sql:
        # Table rename (rare): metadata update is enough; data stays put
        print(f"  [MIGRATE] {fname}: SQL table relabel {old_detail} -> {new_detail} "
              f"(no data move needed)")

    elif old_is_mongo and new_is_mongo:
        # Embed ↔ Reference within Mongo — complex; log and skip for now
        print(f"  [MIGRATE] {fname}: Mongo relabel {old_detail} -> {new_detail} "
              f"(manual migration may be needed)")

    else:
        print(f"  [MIGRATE] {fname}: unhandled path "
              f"{old_detail} -> {new_detail} (skipped)")


# ---------------------------------------------------------------------------
# 7. Main entry point
# ---------------------------------------------------------------------------

def check_and_migrate(meta: dict) -> tuple[dict, list[str]]:
    """
    Called after every CRUD operation.

    Steps
    -----
    1. Recount occurrence_count from live databases
    2. Update total_records + occurrence_counts in meta
    3. Snapshot current backend assignments
    4. Re-run classification silently
    5. Diff old vs new
    6. Migrate each changed field
    7. Re-run Phase 5 + 6 to refresh key_management
    8. Save updated metadata_store.json
    9. Return (updated_meta, list_of_migrated_field_names)
    """
    total, counts = _recount_occurrences(meta)

    # Update meta occurrence stats
    meta["total_records"] = total
    for fname, cnt in counts.items():
        if fname in meta["fields"]:
            meta["fields"][fname]["occurrence_count"] = cnt

    if total == 0:
        # Nothing to classify yet
        return meta, []

    # Snapshot before reclassification
    old_snap = _snapshot_backends(meta)

    # Reclassify silently
    new_snap = _reclassify_silent(meta)

    # Detect changes (top-level only)
    changes = _find_changes(old_snap, new_snap, meta)

    if not changes:
        # Save updated occurrence counts even if no backend changed
        with open(METADATA_FILE, "w") as f:
            json.dump(meta, f, indent=2)
        return meta, []

    # Print migration plan
    print("\n" + "=" * 62)
    print("  RECLASSIFICATION  -  backend changes detected")
    print("=" * 62)
    for fname, old_det, new_det in changes:
        print(f"  {fname:<25}  {old_det}  ->  {new_det}")
    print()

    # Migrate each changed field
    migrated_fields = []
    for fname, old_det, new_det in changes:
        try:
            _migrate_field(fname, old_det, new_det, meta)
            migrated_fields.append(fname)
        except Exception as exc:
            print(f"  [MIGRATE] ERROR for '{fname}': {exc}")

    # Rebuild key_management and re-stamp storage_detail on children
    key_map = phase5_key_management(meta)
    phase6_storage_map(meta, key_map)

    # Save
    with open(METADATA_FILE, "w") as f:
        json.dump(meta, f, indent=2)

    print(f"\n  Migration complete. {len(migrated_fields)} field(s) moved.")
    print("=" * 62)

    return meta, migrated_fields
