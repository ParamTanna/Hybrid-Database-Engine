"""
buffer_store.py  -  Two-Layer Buffer Architecture
==================================================

STAGING BUFFER  (local file — buffer.json)
-------------------------------------------
  Used ONLY during data ingestion (Phase 2).
  All incoming records are written here first because it is fast (no
  network round-trip) and because classification has not happened yet so
  we do not know which backend each field belongs to.
  Cleared automatically after DB Init.

PERSISTENT BUFFER  (MongoDB collection — hybrid_db.buffer)
------------------------------------------------------------
  Used after DB Init and during all CRUD operations.
  Contains ONLY records / fields that have NO SQL or Mongo home:
    • global_key           — always present (for joining)
    • Buffer-classified fields — frequency below threshold
    • unknown_top          — fields not present in the schema at all
    • discarded            — type-mismatch audit trail
    • received_at          — timestamp

  Empty records (no unknown_top, no Buffer fields, no discarded) are
  NOT written here — they carry no useful information.

Transition
----------
  db_init.py / _run_db_init() calls flush_staging_to_mongo(meta) which:
    1. Reads buffer.json
    2. Keeps only {global_key + Buffer fields + unknown_top + …} per record
    3. Inserts those records into MongoDB buffer collection
    4. Deletes buffer.json

"""

import json
import os

from hybriddb.config import paths

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

BUFFER_FILE   = paths.BUFFER_FILE    # staging layer (local file)

MONGO_URI     = paths.MONGO_URI
MONGO_DB_NAME = paths.MONGO_DB_NAME
BUFFER_COLL   = paths.BUFFER_COLL    # persistent layer (MongoDB collection)


# ═════════════════════════════════════════════════════════════════════════════
# STAGING BUFFER  (local file, ingestion-time only)
# ═════════════════════════════════════════════════════════════════════════════

def staging_load() -> dict:
    """Load the staging buffer from buffer.json. Returns {total_buffered, records}."""
    if os.path.exists(BUFFER_FILE):
        with open(BUFFER_FILE, "r") as f:
            return json.load(f)
    return {"total_buffered": 0, "records": []}


def staging_save(buf: dict) -> None:
    """Save the staging buffer dict to buffer.json."""
    with open(BUFFER_FILE, "w") as f:
        json.dump(buf, f, indent=2)


def staging_append(record: dict) -> None:
    """Append a single record to buffer.json."""
    buf = staging_load()
    buf["records"].append({k: v for k, v in record.items() if k != "_id"})
    buf["total_buffered"] += 1
    staging_save(buf)


def staging_append_many(records: list[dict]) -> None:
    """Bulk-append records to buffer.json (used at the end of an ingestion batch)."""
    if not records:
        return
    buf = staging_load()
    for rec in records:
        buf["records"].append({k: v for k, v in rec.items() if k != "_id"})
    buf["total_buffered"] = len(buf["records"])
    staging_save(buf)


def staging_count() -> int:
    """Number of records currently in buffer.json."""
    return staging_load().get("total_buffered", 0)


def staging_clear() -> int:
    """Delete buffer.json. Returns the number of records that were removed."""
    buf = staging_load()
    n   = buf.get("total_buffered", 0)
    if os.path.exists(BUFFER_FILE):
        os.remove(BUFFER_FILE)
    return n


def staging_exists() -> bool:
    return os.path.exists(BUFFER_FILE)


# ═════════════════════════════════════════════════════════════════════════════
# PERSISTENT BUFFER  (MongoDB collection, used after DB Init and for CRUD)
# ═════════════════════════════════════════════════════════════════════════════

def _get_col():
    """
    Returns (client, collection).
    Caller must call client.close().
    Raises if MongoDB is unreachable.
    """
    from pymongo import MongoClient
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3_000)
    client.admin.command("ping")
    return client, client[MONGO_DB_NAME][BUFFER_COLL]


def count() -> int:
    """Total records in the MongoDB buffer collection."""
    client = None
    try:
        client, col = _get_col()
        n = col.count_documents({})
        return n
    except Exception:
        return 0
    finally:
        if client is not None:
            client.close()


def append_record(record: dict) -> None:
    """Insert one record into the MongoDB buffer collection."""
    doc = {k: v for k, v in record.items() if k != "_id"}
    client = None
    try:
        client, col = _get_col()
        col.insert_one(doc)
    except Exception as exc:
        print(f"  [BUFFER] MongoDB unavailable — record not stored in persistent buffer: {exc}")
    finally:
        if client is not None:
            client.close()


def append_many(records: list[dict]) -> None:
    """Bulk-insert records into MongoDB buffer."""
    if not records:
        return
    docs = [{k: v for k, v in r.items() if k != "_id"} for r in records]
    client = None
    try:
        client, col = _get_col()
        col.insert_many(docs)
    except Exception as exc:
        print(f"  [BUFFER] MongoDB unavailable — {len(docs)} records not stored in persistent buffer: {exc}")
    finally:
        if client is not None:
            client.close()


def find_records(where: dict | None = None) -> list[dict]:
    """
    Query the MongoDB buffer collection.
    where=None returns all records. Returns list of dicts (no _id).
    """
    client = None
    try:
        client, col = _get_col()
        records = list(col.find(where or {}, {"_id": 0}))
        return records
    except Exception:
        return []
    finally:
        if client is not None:
            client.close()


def remove_records(where: dict) -> int:
    """
    Delete records matching where from MongoDB buffer.
    where values can be lists (treated as $in).
    Returns count deleted.
    """
    mongo_filter = {
        k: ({"$in": v} if isinstance(v, list) else v)
        for k, v in where.items()
    }
    client = None
    try:
        client, col = _get_col()
        res = col.delete_many(mongo_filter)
        return res.deleted_count
    except Exception:
        return 0
    finally:
        if client is not None:
            client.close()


def update_records_func(filter_fn, update_fn) -> int:
    """
    Load all MongoDB buffer records, apply filter_fn/update_fn in Python,
    write back modified records. Used for in-place field removal.
    Returns number of records modified.
    """
    client = None
    try:
        client, col = _get_col()
        # Fetch with _id so each record can be matched by its unique MongoDB
        # identifier rather than by its entire content (fragile if any value
        # changes or duplicate records exist). No global key is available in
        # this function's scope (it only receives opaque filter/update
        # callables), so _id is the safest stable identity to filter on.
        all_recs = list(col.find({}))
        modified = 0
        for rec in all_recs:
            if filter_fn(rec):
                updated = update_fn(rec)
                col.replace_one(
                    {"_id": rec["_id"]},
                    {k: v for k, v in updated.items() if k != "_id"},
                )
                modified += 1
        return modified
    except Exception:
        return 0
    finally:
        if client is not None:
            client.close()


def clear() -> int:
    """Delete ALL records from MongoDB buffer. Returns count removed."""
    client = None
    try:
        client, col = _get_col()
        n = col.count_documents({})
        col.delete_many({})
        return n
    except Exception:
        return 0
    finally:
        if client is not None:
            client.close()


def ensure_index(global_key: str) -> None:
    """Create an index on global_key for faster lookups (idempotent)."""
    client = None
    try:
        client, col = _get_col()
        col.create_index(global_key)
    except Exception:
        pass
    finally:
        if client is not None:
            client.close()


# ═════════════════════════════════════════════════════════════════════════════
# TRANSITION: flush staging → persistent buffer
# ═════════════════════════════════════════════════════════════════════════════

def flush_staging_to_mongo(meta: dict) -> int:
    """
    Called once at the end of DB Init.

    Reads every record in buffer.json (staging), extracts only the
    buffer-relevant portion:
        { global_key, <Buffer-classified fields>, unknown_top, discarded, received_at }
    and inserts those slim records into the MongoDB buffer collection.

    Records that have nothing buffer-worthy (all fields were SQL/Mongo,
    unknown_top is empty, discarded is empty) are dropped silently.

    Clears buffer.json when done.

    Returns the number of records inserted into the MongoDB buffer.
    """
    global_key  = meta.get("global_key", "")
    meta_fields = meta.get("fields", {})
    keep_always = {"unknown_top", "discarded", "received_at"}

    # Top-level field names classified to Buffer
    buffer_fields = {
        fname
        for fname, fdata in meta_fields.items()
        if "." not in fname
        and fdata.get("storage_backend") == "Buffer"
        and fname not in keep_always
        and fname != global_key
    }

    staging = staging_load()
    to_insert: list[dict] = []

    for rec in staging.get("records", []):
        gk_val = rec.get(global_key)
        if gk_val is None:
            continue

        buf_rec: dict = {global_key: gk_val}

        # Copy Buffer-classified fields
        for fname in buffer_fields:
            if fname in rec:
                buf_rec[fname] = rec[fname]

        # Copy metadata / audit keys (only if non-empty)
        for key in keep_always:
            val = rec.get(key)
            if val:                               # skip empty dicts / None
                buf_rec[key] = val

        # Only insert if there is something useful beyond the global key itself
        has_content = (
            buf_rec.get("unknown_top") or         # has unknown fields
            buf_rec.get("discarded") or           # has failed validations
            any(k not in {global_key} | keep_always for k in buf_rec)  # buffer fields
        )

        if has_content:
            to_insert.append(buf_rec)

    if to_insert:
        ensure_index(global_key)
        append_many(to_insert)

    # Remove the staging file
    staging_clear()

    inserted = len(to_insert)
    total_staged = staging.get("total_buffered", 0)
    print(f"  [BUFFER] Staging flushed: {total_staged} staged records -> "
          f"{inserted} stored in MongoDB buffer (remainder had no buffer content).")
    return inserted


# ═════════════════════════════════════════════════════════════════════════════
# Bulk load/save for the MongoDB persistent buffer
# (used by delete_operation and reclassify_migrate which need load-all/save-all)
# ═════════════════════════════════════════════════════════════════════════════

def load_buffer() -> dict:
    """
    Load all records from the MongoDB persistent buffer.
    Returns {"total_buffered": N, "records": [...]}.
    Used by CRUD and migration code that needs a full in-memory view.
    """
    records = find_records()
    return {"total_buffered": len(records), "records": records}


def save_buffer(buf: dict) -> None:
    """
    Replace the entire MongoDB persistent buffer with buf["records"].
    Used by CRUD and migration code after in-place modifications.
    """
    records = buf.get("records", [])
    client = None
    try:
        client, col = _get_col()
        col.delete_many({})
        if records:
            docs = [{k: v for k, v in r.items() if k != "_id"} for r in records]
            col.insert_many(docs)
    except Exception as exc:
        print(f"  [BUFFER] MongoDB save_buffer failed: {exc}")
    finally:
        if client is not None:
            client.close()


# ═════════════════════════════════════════════════════════════════════════════
# Export: dump MongoDB buffer to buffer_export.json for inspection / backup
# ═════════════════════════════════════════════════════════════════════════════

def export_to_file(path: str = "buffer_export.json") -> int:
    """Write current MongoDB buffer records to a JSON file. Returns record count."""
    records = find_records()
    with open(path, "w") as f:
        json.dump({"total_buffered": len(records), "records": records}, f, indent=2)
    return len(records)
