"""
main.py  -  Hybrid Database Interactive Hub
============================================
Single entry-point for the entire hybrid database pipeline.

Menu options
------------
  1. Schema Registration / Update
  2. Data Ingestion  (auto-runs Classification + DB Init after ingestion)
  3. CRUD  (read / insert / update / delete  via JSON query)
  4. Flush Buffer    (clear buffer.json, keep SQL & Mongo intact)
  5. Reset Everything  (wipe DB, buffer, and metadata stats)
  0. Exit

Run:
    python main.py
"""

import json
import os
import sys
import sqlite3
import requests
from datetime import datetime, timezone

# ── Pipeline phases ─────────────────────────────────────────────────────────
from phase1_schema_registration import build_metadata

from phase2_data_ingestion import (
    process_record,
    _update_constraints_from_data,
)
from buffer_store import (
    # Staging (local file) — used during ingestion
    staging_load        as _load_buffer,
    staging_save        as _save_buffer,
    staging_count       as _staging_count,
    staging_clear       as _staging_clear,
    BUFFER_FILE,
    # Persistent (MongoDB) — used after DB Init and for CRUD
    count               as _buf_count,
    clear               as _buf_clear,
    flush_staging_to_mongo as _flush_staging,
)

from classification import (
    phase3_field_analysis,
    phase4_classify,
    phase5_key_management,
    phase6_storage_map,
    _main_table_name,
)

from db_init import (
    setup_sqlite,
    setup_mongo,
    insert_sql,
    insert_mongo,
)

# ── CRUD operations ──────────────────────────────────────────────────────────
from read_operation   import execute_read
from insert_operation import execute_insert
from update_operation import execute_update
from delete_operation import execute_delete

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_FILE   = "schema.json"
METADATA_FILE = "metadata_store.json"
SQLITE_FILE   = "hybrid_db.db"
MONGO_URI     = "mongodb://localhost:27017"
MONGO_DB_NAME = "hybrid_db"
STREAM_BASE   = "http://127.0.0.1:8000/record"
DEFAULT_COUNT = 100

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

W = 64   # banner width

def _banner(title: str, char: str = "="):
    print("\n" + char * W)
    print(f"  {title}")
    print(char * W)


def _load_json(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def _save_json(path: str, data: dict):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def _load_metadata(required: bool = True) -> dict | None:
    if os.path.exists(METADATA_FILE):
        return _load_json(METADATA_FILE)
    if required:
        print(f"\n  [ERROR] {METADATA_FILE} not found.")
        print("  Run option 1 (Schema Registration) first.")
        return None
    return {}


def _confirm(prompt: str) -> bool:
    ans = input(f"\n  {prompt} (yes/no): ").strip().lower()
    return ans in ("yes", "y")


# ---------------------------------------------------------------------------
# 1. Schema Registration / Update
# ---------------------------------------------------------------------------

def menu_schema_registration():
    _banner("SCHEMA REGISTRATION / UPDATE")

    if not os.path.exists(SCHEMA_FILE):
        print(f"\n  [ERROR] {SCHEMA_FILE} not found in current directory.")
        return

    raw_schema = _load_json(SCHEMA_FILE)
    metadata   = build_metadata(raw_schema)
    _save_json(METADATA_FILE, metadata)

    print(f"\n  global_key : {metadata['global_key']}")
    print(f"  registered : {metadata['registered_at']}")
    print(f"  fields     : {len(metadata['fields'])} registered\n")

    for fname, fdata in metadata["fields"].items():
        indent  = "    " * fdata["level"]
        flags   = []
        if fdata.get("appendable")        is not None: flags.append(f"appendable={fdata['appendable']}")
        if fdata.get("independent_query") is not None: flags.append(f"iq={fdata['independent_query']}")
        if fdata.get("one_to_many")       is not None: flags.append(f"1tm={fdata['one_to_many']}")
        flag_str = f"  [{', '.join(flags)}]" if flags else ""
        print(f"  {indent}{fname}  ({fdata['type']}){flag_str}")

    print(f"\n  Saved -> {METADATA_FILE}")


# ---------------------------------------------------------------------------
# 2. Data Ingestion
# ---------------------------------------------------------------------------

def menu_ingestion():
    _banner("DATA INGESTION")

    meta = _load_metadata()
    if meta is None:
        return

    raw = input(f"\n  How many records to ingest? [default {DEFAULT_COUNT}]: ").strip()
    try:
        target = int(raw) if raw else DEFAULT_COUNT
        if target <= 0:
            raise ValueError
    except ValueError:
        print(f"  Invalid input — using default {DEFAULT_COUNT}.")
        target = DEFAULT_COUNT

    url = f"{STREAM_BASE}/{target}"
    print(f"\n  Connecting to {url} ...\n")

    # Load existing staging buffer (may have prior records)
    buf         = _load_buffer()
    seen_values : dict = {}
    ingested    = 0
    n_unknown   = 0
    n_discarded = 0
    n_validated = 0

    try:
        with requests.get(url, stream=True, timeout=60) as resp:
            resp.raise_for_status()

            for raw_line in resp.iter_lines():
                if ingested >= target:
                    break
                if not raw_line:
                    continue

                line = raw_line.decode("utf-8") if isinstance(raw_line, bytes) else raw_line
                if not line.startswith("data:"):
                    continue

                try:
                    record = json.loads(line[5:].strip())
                except json.JSONDecodeError:
                    continue

                entry = process_record(record, meta, seen_values)
                buf["records"].append(entry)
                buf["total_buffered"] += 1
                ingested    += 1
                n_fields     = len(entry) - 3
                n_unknown   += len(entry["unknown_top"])
                n_discarded += len(entry["discarded"])
                n_validated += n_fields

                print(f"  [{ingested:>4}/{target}]  "
                      f"{meta['global_key']}={entry.get(meta['global_key'])}  "
                      f"fields={n_fields}  "
                      f"unknown={len(entry['unknown_top'])}  "
                      f"discarded={len(entry['discarded'])}")

    except requests.exceptions.ConnectionError:
        print(f"\n  [ERROR] Cannot reach simulation server at {STREAM_BASE}")
        print("  Start it first:  python simulation_code.py")
        return

    _update_constraints_from_data(meta, seen_values)
    _save_buffer(buf)            # write to buffer.json (staging)
    _save_json(METADATA_FILE, meta)

    print(f"\n  Records ingested    : {ingested}")
    print(f"  Total in staging    : {buf['total_buffered']}  ({BUFFER_FILE})")
    print(f"  Fields validated    : {n_validated}")
    print(f"  Unknown (buffered)  : {n_unknown}")
    print(f"  Discarded           : {n_discarded}")
    print(f"  Metadata saved -> {METADATA_FILE}")

    # ── Auto-run Classification + DB Init ──────────────────────────────────
    print("\n  Ingestion complete. Running Classification and DB Init automatically...")
    meta = _run_classification(meta)
    _run_db_init(meta)


# ---------------------------------------------------------------------------
# Internal helpers for Classification and DB Init (called automatically)
# ---------------------------------------------------------------------------

def _run_classification(meta: dict) -> dict:
    _banner("CLASSIFICATION  (Phases 3-6)")

    freq_map = phase3_field_analysis(meta)
    phase4_classify(meta, freq_map)
    key_map  = phase5_key_management(meta)
    phase6_storage_map(meta, key_map)
    _save_json(METADATA_FILE, meta)

    km_sql   = meta.get("key_management", {}).get("SQL", {})
    km_mongo = meta.get("key_management", {}).get("Mongo", {})

    for tbl in km_sql:
        pk = km_sql[tbl].get("primary_key", "?")
        print(f"  SQL table   : {tbl}  (PK={pk})")

    main_col   = _main_table_name(meta["global_key"])
    ref_tops   = [f for f in km_mongo.get("reference", []) if "." not in f]
    embed_flds = km_mongo.get("embed", [])
    print(f"  Mongo embed : {main_col}  fields={embed_flds}")
    for col in ref_tops:
        print(f"  Mongo ref   : {col}")
    print(f"\n  Metadata saved -> {METADATA_FILE}")
    return meta


def _run_db_init(meta: dict):
    _banner("DB INITIALISE")

    if not meta.get("key_management"):
        print("\n  [ERROR] Classification not available — skipping DB init.")
        return

    sql_tables               = setup_sqlite(meta)
    main_collection, ref_top = setup_mongo(meta)

    from buffer_store import staging_load as _stg_load, flush_staging_to_mongo as _flush
    staging = _stg_load()
    records = staging.get("records", [])

    if not records:
        print(f"\n  [WARN] Staging buffer ({BUFFER_FILE}) is empty — databases will be empty.")
    else:
        print(f"\n  Loading {len(records)} records from staging buffer ({BUFFER_FILE}) ...")

    insert_sql(meta, sql_tables, records)
    insert_mongo(meta, main_collection, ref_top, records)

    # Flush buffer-classified + unknown fields to MongoDB, clear buffer.json
    print("\n  Flushing staging buffer -> MongoDB persistent buffer ...")
    _flush(meta)


# ---------------------------------------------------------------------------
# 3. CRUD  (JSON query dispatch)
# ---------------------------------------------------------------------------

_CRUD_HINT = """\
  Accepted operations and example queries
  ----------------------------------------
  READ:
    {"operation":"read","fields":["name","orders"],"where":{"customer_id":12345}}

  INSERT:
    {"operation":"insert","data":{"customer_id":99999,"name":"Alice","email":"a@b.com"}}

  UPDATE (full record):
    {"operation":"update","where":{"customer_id":99999},"data":{"name":"Alice2"}}

  UPDATE (entity):
    {"operation":"update","entity":"orders","where":{"customer_id":99999,"order_id":101},"data":{"amount":999}}

  DELETE (full record):
    {"operation":"delete","where":{"customer_id":99999}}

  DELETE (entity):
    {"operation":"delete","entity":"orders","where":{"customer_id":99999,"order_id":101}}
"""

def menu_crud():
    _banner("CRUD  —  JSON Query Interface")

    meta = _load_metadata()
    if meta is None:
        return

    while True:
        print()
        print("  Enter a JSON query (or 'help' / 'back' to return to menu):")
        raw = input("  Query> ").strip()

        if raw.lower() in ("back", "b", "q", "exit", ""):
            return

        if raw.lower() == "help":
            print(_CRUD_HINT)
            continue

        try:
            query = json.loads(raw)
        except json.JSONDecodeError as e:
            print(f"\n  [ERROR] Invalid JSON: {e}")
            continue

        op = query.get("operation", "").lower()

        if op == "read":
            results = execute_read(query, meta)
            print("\n  Result:")
            print(json.dumps(results, indent=4, default=str))

        elif op == "insert":
            execute_insert(query, meta)
            # Reload meta — reclassify_migrate may have updated backends
            meta = _load_metadata() or meta

        elif op == "update":
            execute_update(query, meta)
            meta = _load_metadata() or meta

        elif op == "delete":
            execute_delete(query, meta)
            meta = _load_metadata() or meta

        else:
            print(f"\n  [ERROR] Unknown operation '{op}'.")
            print("  Valid: read | insert | update | delete")


# ---------------------------------------------------------------------------
# 6. Flush Buffer  (empty buffer.json, keep SQL + Mongo intact)
# ---------------------------------------------------------------------------

def menu_flush_buffer():
    _banner("FLUSH BUFFER")

    n_mongo   = _buf_count()
    n_staging = _staging_count()

    if n_mongo == 0 and n_staging == 0:
        print("\n  Both buffers are already empty — nothing to flush.")
        return

    msg = []
    if n_mongo   > 0: msg.append(f"{n_mongo} record(s) in MongoDB buffer")
    if n_staging > 0: msg.append(f"{n_staging} record(s) in staging ({BUFFER_FILE})")

    if not _confirm(f"This will remove {' and '.join(msg)}. Confirm?"):
        print("  Aborted.")
        return

    if n_mongo > 0:
        removed = _buf_clear()
        print(f"  MongoDB buffer  : {removed} record(s) removed.")
    if n_staging > 0:
        _staging_clear()
        print(f"  Staging buffer  : {BUFFER_FILE} deleted.")
    print("  SQL and MongoDB (main/reference) collections are NOT affected.")


# ---------------------------------------------------------------------------
# 7. Reset Everything  (wipe SQL DB, buffer, and metadata stats)
# ---------------------------------------------------------------------------

def _drop_mongo_collections(meta: dict):
    try:
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.admin.command("ping")
        db = client[MONGO_DB_NAME]

        global_key = meta.get("global_key", "")
        main_col   = _main_table_name(global_key) if global_key else None
        km_mongo   = meta.get("key_management", {}).get("Mongo", {})
        ref_tops   = [f for f in km_mongo.get("reference", []) if "." not in f]

        dropped = []
        if main_col and main_col in db.list_collection_names():
            db.drop_collection(main_col)
            dropped.append(main_col)
        for col in ref_tops:
            if col in db.list_collection_names():
                db.drop_collection(col)
                dropped.append(col)

        client.close()
        if dropped:
            print(f"  MongoDB  : dropped {dropped}")
        else:
            print("  MongoDB  : no collections to drop")

    except Exception:
        print(f"  MongoDB  : unreachable — skipped")


def menu_reset():
    _banner("RESET EVERYTHING", char="!")

    print("\n  This will permanently destroy:")
    print(f"    - {SQLITE_FILE}            (SQLite database)")
    print(f"    - MongoDB buffer collection (all buffered records)")
    print(f"    - {METADATA_FILE}  (entire schema + stats + classifications)")
    print("    - MongoDB collections       (customers, reviews, ...)")

    if not _confirm("Are you SURE you want to reset everything?"):
        print("  Aborted.")
        return

    meta = _load_metadata(required=False) or {}

    # 1. Delete SQLite DB
    if os.path.exists(SQLITE_FILE):
        os.remove(SQLITE_FILE)
        print(f"  SQLite   : {SQLITE_FILE} deleted")
    else:
        print(f"  SQLite   : {SQLITE_FILE} not found — skipped")

    # 2. Drop MongoDB collections
    _drop_mongo_collections(meta)

    # 3. Clear both buffers
    removed = _buf_clear()
    print(f"  Mongo buf: MongoDB buffer cleared ({removed} records removed)")
    _staging_clear()
    print(f"  Staging  : {BUFFER_FILE} cleared")

    # 4. Delete metadata entirely (schema + stats + classifications all go)
    if os.path.exists(METADATA_FILE):
        os.remove(METADATA_FILE)
        print(f"  Metadata : {METADATA_FILE} deleted")
    else:
        print(f"  Metadata : {METADATA_FILE} not found — skipped")

    print("\n  Reset complete.  Run option 1 (Schema Registration) to start fresh.")


# ---------------------------------------------------------------------------
# Status sidebar  (shown in menu header)
# ---------------------------------------------------------------------------

def _status_line() -> str:
    parts = []

    if os.path.exists(METADATA_FILE):
        try:
            m  = _load_json(METADATA_FILE)
            gk = m.get("global_key", "?")
            n  = m.get("total_records", 0)
            parts.append(f"schema=ok  global_key={gk}  records={n}")
            if m.get("key_management"):
                parts.append("classified=yes")
            else:
                parts.append("classified=no")
        except Exception:
            parts.append("schema=error")
    else:
        parts.append("schema=none")

    if os.path.exists(SQLITE_FILE):
        try:
            conn  = sqlite3.connect(SQLITE_FILE)
            tbls  = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
            conn.close()
            parts.append(f"sql=ok({','.join(tbls)})")
        except Exception:
            parts.append("sql=error")
    else:
        parts.append("sql=none")

    try:
        mongo_n   = _buf_count()
        staging_n = _staging_count()
        parts.append(f"buffer(mongo)={mongo_n} | staging={staging_n}")
    except Exception:
        parts.append("buffer=error")

    return "  " + " | ".join(parts)


# ---------------------------------------------------------------------------
# Main menu loop
# ---------------------------------------------------------------------------

_MENU = """
  Options
  -------
  1.  Schema Registration / Update
  2.  Data Ingestion   (auto-runs Classification + DB Init after)
  3.  CRUD             (read / insert / update / delete)
  4.  Reset Everything (wipe DB + buffer + metadata)
  0.  Exit
"""

_ACTIONS = {
    "1": menu_schema_registration,
    "2": menu_ingestion,
    "3": menu_crud,
    "4": menu_reset,
}


def _full_pipeline():
    """Option triggered by passing --pipeline flag. Runs 1→2→3→4 in sequence."""
    _banner("FULL PIPELINE RUN", char="#")

    raw = input(f"\n  Records to ingest? [default {DEFAULT_COUNT}]: ").strip()
    try:
        target = int(raw) if raw else DEFAULT_COUNT
        if target <= 0:
            raise ValueError
    except ValueError:
        target = DEFAULT_COUNT

    meta = build_metadata(_load_json(SCHEMA_FILE))
    _save_json(METADATA_FILE, meta)

    # Ingestion auto-triggers classification + db init
    _run_ingestion_with_target(meta, target)
    meta = _load_json(METADATA_FILE)
    meta = _run_classification(meta)
    _run_db_init(meta)

    _banner("PIPELINE COMPLETE", char="#")
    print(f"  Records ingested : {meta['total_records']}")
    print(f"  Fields tracked   : {len(meta['fields'])}")


def _run_ingestion_with_target(meta: dict, target: int):
    """Shared ingestion helper used by _full_pipeline."""
    url         = f"{STREAM_BASE}/{target}"
    buf         = _load_buffer()       # load staging buffer
    seen_values = {}
    ingested    = 0

    print(f"\n  Ingesting {target} records from {url} ...")
    try:
        with requests.get(url, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            for raw_line in resp.iter_lines():
                if ingested >= target:
                    break
                if not raw_line:
                    continue
                line = raw_line.decode("utf-8") if isinstance(raw_line, bytes) else raw_line
                if not line.startswith("data:"):
                    continue
                try:
                    record = json.loads(line[5:].strip())
                except json.JSONDecodeError:
                    continue
                entry = process_record(record, meta, seen_values)
                buf["records"].append(entry)
                buf["total_buffered"] += 1
                ingested += 1
                print(f"  [{ingested:>4}/{target}]  {meta['global_key']}="
                      f"{entry.get(meta['global_key'])}")
    except requests.exceptions.ConnectionError:
        print(f"\n  [ERROR] Cannot reach {STREAM_BASE}")
        return

    _update_constraints_from_data(meta, seen_values)
    _save_buffer(buf)          # write to buffer.json (staging)
    _save_json(METADATA_FILE, meta)
    print(f"  Ingested {ingested} records -> staging buffer ({BUFFER_FILE}).")


if __name__ == "__main__":
    # Optional --pipeline flag runs the full pipeline non-interactively
    if "--pipeline" in sys.argv:
        _full_pipeline()
        sys.exit(0)

    print("\n" + "=" * W)
    print("  HYBRID DATABASE  —  Interactive Hub")
    print("=" * W)

    while True:
        # Status line
        print("\n" + "-" * W)
        print("  Status:")
        print(_status_line())
        print("-" * W)

        print(_MENU)
        choice = input("  Select> ").strip()

        if choice in ("0", "exit", "quit", "q"):
            print("\n  Goodbye.\n")
            break

        action = _ACTIONS.get(choice)
        if action:
            action()
        else:
            print(f"\n  Unknown option '{choice}' — enter 1-7 or 0 to exit.")
