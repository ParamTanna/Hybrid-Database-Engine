import json
import sys
import os
from pathlib import Path
import httpx
from typing import Any

# ── Anchor all relative paths to the directory containing this file ──────────
# config.py uses sqlite:///./data/hybrid.db and DATA_DIR = Path("./data"),
# both of which are relative to the CWD at import time.  If the process is
# launched from any directory other than the project root those paths split:
# the ingest engine creates ./data/hybrid.db in one place while execute_read
# opens a fresh empty file somewhere else.  Forcing an absolute path here,
# before config is imported, eliminates the split-brain entirely.
_PROJECT_ROOT = Path(__file__).resolve().parent
_ABS_DATA_DIR = _PROJECT_ROOT / "data"
_ABS_DATA_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("HYBRID_SQL_URL", f"sqlite:///{_ABS_DATA_DIR / 'hybrid.db'}")

import hybrid_framework.config as config

# Patch the live config module so every other module that imports config
# also sees the absolute paths (they all do `import hybrid_framework.config as config`
# and Python caches the same module object).
config.DATA_DIR      = _ABS_DATA_DIR
config.METADATA_FILE = _ABS_DATA_DIR / "metadata.json"
config.BUFFER_FILE   = _ABS_DATA_DIR / "buffer.json"
config.SQL_URL       = os.environ["HYBRID_SQL_URL"]
import hybrid_framework.metadata_manager as metadata_manager
import hybrid_framework.schema_registry as schema_registry
import hybrid_framework.analysis as analysis
import hybrid_framework.classification as classification
import hybrid_framework.ingest as ingest
import hybrid_framework.normalization_engine as normalization_engine
import hybrid_framework.mongo_strategy as mongo_strategy
import hybrid_framework.crud as crud
import hybrid_framework.buffer_manager as buffer_manager
import hybrid_framework.query_engine as query_engine

# Module-level rolling sample for FD detection (kept in-memory across menu iterations)
_record_sample: list[dict] = []


def print_header() -> None:
    print("""
╔══════════════════════════════════════════╗
║   Hybrid Database Framework — CS432     ║
║   SQL + MongoDB Adaptive Ingestion      ║
║   Normalisation: 1NF → 2NF → 3NF       ║
╚══════════════════════════════════════════╝
""")


def startup_checks() -> crud.CRUDManager:
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)

    crud_mgr = crud.CRUDManager()

    if not crud_mgr.mongo_available:
        print("Warning: MongoDB unavailable. SQL-only mode.")

    try:
        httpx.get("http://127.0.0.1:8000/", timeout=2.0)
    except Exception:
        print("Warning: Simulation API not reachable at port 8000. Ingestion will fail.")

    return crud_mgr


def fetch_stream_sse(url: str, count: int) -> list[dict]:
    records: list[dict] = []
    try:
        with httpx.stream("GET", url.format(count=count), timeout=None) as response:
            for line in response.iter_lines():
                if line.startswith("data: "):
                    records.append(json.loads(line[len("data: "):]))
                    if len(records) >= count:
                        break
    except Exception as e:
        print(f"Error fetching from stream: {e}")
    return records


def show_placement_summary() -> None:
    placement = metadata_manager.get_field_placement()
    if not placement:
        print("\nNo field placement decisions yet.")
        return

    dim_tables = metadata_manager.get_3nf_dimension_tables()
    dim_fields: set[str] = set()
    for dim_info in dim_tables.values():
        dim_fields.update(dim_info["all_fields"])

    print("\nField Placement Summary:")
    print(f"{'Field':<25} {'Backend':<10} {'Target':<30} {'Reason'}")
    print("-" * 85)
    for field, info in sorted(placement.items()):
        backend = info["backend"].upper()
        target  = info.get("table") or info.get("collection") or "—"
        # Annotate 3NF dimension fields
        if info.get("dimension_table"):
            target = f"{target} → {info['dimension_table']} (FK)"
        reason  = info.get("reason", "")
        print(f"{field:<25} {backend:<10} {target:<30} {reason}")

    if dim_tables:
        print("\n3NF Dimension Tables:")
        for dim_name, dim_info in dim_tables.items():
            print(f"  {dim_name}: PK={dim_info['determinant']}, "
                  f"dependent={dim_info['dependent_fields']}")


def main_menu() -> None:
    crud_mgr   = startup_checks()
    buffer_mgr = buffer_manager.BufferManager(crud_mgr)
    q_engine   = query_engine.QueryEngine(crud_mgr, buffer_mgr)

    while True:
        print("\n═══════════════════════════════")
        print("  Main Menu")
        print("═══════════════════════════════")
        print("  [1] Register / update schema")
        print("  [2] Ingest records from stream")
        print("  [3] Query (CRUD operations)")
        print("  [4] View placement metadata")
        print("  [5] Flush buffer")
        print("  [6] Reset all data")
        print("  [0] Exit")

        choice = input("\nChoice: ").strip()

        # ── [1] Schema registration ──────────────────────────────────────────
        if choice == "1":
            print("\nEnter schema as JSON, or type 'file' to load from a .json file path:")
            inp = input("> ").strip()
            if inp.lower() == "file":
                path = input("File path: ").strip()
                try:
                    with open(path, "r") as f:
                        schema_dict = json.load(f)
                except Exception as e:
                    print(f"Error: {e}")
                    continue
            else:
                print("Enter JSON (type 'END' on a new line to finish):")
                lines = [inp]
                while True:
                    line = input()
                    if line.strip() == "END":
                        break
                    lines.append(line)
                try:
                    schema_dict = json.loads("".join(lines))
                except Exception as e:
                    print(f"Error: {e}")
                    continue

            try:
                summary = schema_registry.register_schema(schema_dict)
                print("\nSchema registered successfully.")
                print(json.dumps(summary, indent=2))

                # Re-run normalisation if data already exists
                cum   = metadata_manager.get_cumulative_stats()
                stats = analysis.cumulative_raw_to_derived(cum)
                if stats:
                    normalization_engine.run_normalization(_record_sample, stats, schema_dict)
                    mongo_fields = [
                        f for f, s in stats.items()
                        if (
                            metadata_manager.get_placement_for_field(f)
                            and metadata_manager.get_placement_for_field(f)["backend"] == "mongo"
                        )
                    ]
                    mongo_strategy.run_mongo_strategy(mongo_fields, stats, schema_dict, _record_sample)
                    crud_mgr.ensure_all_tables(metadata_manager.get_sql_tables())
            except ValueError as e:
                print(f"Error: {e}")

        # ── [2] Ingest from stream ───────────────────────────────────────────
        elif choice == "2":
            count_str = input("How many records to fetch? (default 100): ").strip()
            count = int(count_str) if count_str.isdigit() else 100

            raw_records = fetch_stream_sse("http://127.0.0.1:8000/record/{count}", count)
            if not raw_records:
                print("No records fetched.")
                continue

            processed_records: list[dict] = []
            for raw in raw_records:
                proc = ingest.ingest_one(raw)
                processed_records.append(proc)
                _record_sample.append(proc)
                if len(_record_sample) > config.FD_SAMPLE_SIZE:
                    _record_sample.pop(0)

            # Update cumulative stats
            batch_stats = analysis.analyze_buffer(processed_records)
            prev_cum    = metadata_manager.get_cumulative_stats()
            merged_raw  = analysis.merge_cumulative_stats(prev_cum, batch_stats, len(processed_records))
            metadata_manager.save_cumulative_stats(merged_raw, merged_raw["total_records"])

            derived   = analysis.cumulative_raw_to_derived(merged_raw)
            decisions = classification.classify_fields(derived)
            metadata_manager.save_field_placement(decisions)

            # Run normalisation (1NF → 2NF → 3NF) and Mongo strategy
            schema = schema_registry.get_schema()
            if schema:
                normalization_engine.run_normalization(_record_sample, derived, schema)
                mongo_fields = [f for f, d in decisions.items() if d["backend"] == "mongo"]
                mongo_strategy.run_mongo_strategy(mongo_fields, derived, schema, _record_sample)
            else:
                # No schema registered yet — build a minimal flat 'records' table
                # directly from the fields classified as SQL so inserts don't get
                # silently dropped by the 'records' not in sql_tables guard.
                sql_fields = {
                    f: {"sql_type": "TEXT", "unique": info.get("unique", False), "not_null": False}
                    for f, info in decisions.items()
                    if info["backend"] == "sql"
                }
                if sql_fields:
                    minimal_tables = {
                        "records": {
                            "columns":      sql_fields,
                            "primary_key":  config.JOIN_KEY,
                            "foreign_keys": [],
                        }
                    }
                    metadata_manager.save_sql_tables(minimal_tables)

            # Always create/sync tables — must happen whether or not a schema
            # is registered, and must happen BEFORE the insert loop below.
            crud_mgr.ensure_all_tables(metadata_manager.get_sql_tables())

            # Insert records
            sql_tables    = metadata_manager.get_sql_tables()
            mongo_colls   = metadata_manager.get_mongo_collections()
            placement     = metadata_manager.get_field_placement()
            flattened     = metadata_manager.get_flattened_objects()

            inserted = 0
            errors   = 0
            for rec in processed_records:
                decided_rec: dict     = {}
                undecided_fields: dict = {}

                for k, v in rec.items():
                    p = placement.get(k, {})
                    if p.get("backend") in ("sql", "mongo"):
                        decided_rec[k] = v
                    else:
                        undecided_fields[k] = v

                # JOIN_KEY must always be in the decided record
                decided_rec[config.JOIN_KEY] = rec[config.JOIN_KEY]

                try:
                    crud_mgr.insert_record(decided_rec, sql_tables, mongo_colls, placement, flattened)
                    inserted += 1
                except Exception as e:
                    errors += 1
                    print(f"  Insert error for {rec.get(config.JOIN_KEY, '?')}: {e}")

                if undecided_fields:
                    buffer_mgr.add_pending_fields(rec[config.JOIN_KEY], undecided_fields)

            print(f"\nIngested {inserted} records. Errors: {errors}.")
            show_placement_summary()

            buf_stats = buffer_mgr.get_buffer_stats()
            print(
                f"\nBuffer status: {len(buf_stats['pending_field_names'])} fields pending "
                f"across {buf_stats['pending_record_count']} records."
            )

        # ── [3] CRUD query ───────────────────────────────────────────────────
        elif choice == "3":
            print("\nQuery Options:")
            print("  [a] Read")
            print("  [b] Insert")
            print("  [c] Delete")
            print("  [d] Update")
            print("  [e] Raw JSON operation")
            print("  [x] Back")
            q_choice = input("\nChoice: ").strip().lower()

            op: dict = {}
            if q_choice == "a":
                op = {"operation": "read", "filters": {}, "fields": None}
                f_name = input("Filter field (Enter to skip): ").strip()
                if f_name:
                    f_val = input("Filter value: ").strip()
                    op["filters"][f_name] = f_val
                f_list = input("Fields to return (comma-separated, Enter for all): ").strip()
                if f_list:
                    op["fields"] = [f.strip() for f in f_list.split(",")]

            elif q_choice == "b":
                print("Enter record JSON (single line):")
                rec_json = input("> ").strip()
                try:
                    op = {"operation": "insert", "record": json.loads(rec_json)}
                except Exception:
                    print("Invalid JSON")
                    continue

            elif q_choice == "c":
                op = {"operation": "delete", "filters": {}}
                f_name = input("Filter field: ").strip()
                f_val  = input("Filter value: ").strip()
                op["filters"][f_name] = f_val

            elif q_choice == "d":
                op = {"operation": "update", "filters": {}, "set": {}}
                f_name = input("Filter field: ").strip()
                f_val  = input("Filter value: ").strip()
                op["filters"][f_name] = f_val
                s_name = input("Update field: ").strip()
                s_val  = input("New value: ").strip()
                op["set"][s_name] = s_val

            elif q_choice == "e":
                print("Enter JSON operation (type 'END' on a new line to finish):")
                lines: list[str] = []
                while True:
                    line = input()
                    if line.strip() == "END":
                        break
                    lines.append(line)
                try:
                    op = json.loads("".join(lines))
                except Exception:
                    print("Invalid JSON")
                    continue

            elif q_choice == "x":
                continue

            if op:
                res = q_engine.handle_query(op)
                print(json.dumps(res, indent=2, default=str))

        # ── [4] View metadata ────────────────────────────────────────────────
        elif choice == "4":
            print(json.dumps(metadata_manager.load(), indent=2, default=str))

        # ── [5] Flush buffer ─────────────────────────────────────────────────
        elif choice == "5":
            confirm = input(
                "Are you sure? This will force all buffered fields to MongoDB. (y/n): "
            ).strip().lower()
            if confirm == "y":
                res = buffer_mgr.force_flush()
                print(f"Flushed: {res.get('flushed_fields', [])}")

        # ── [6] Reset all data ───────────────────────────────────────────────
        elif choice == "6":
            confirm = input(
                "Are you sure? This deletes ALL data, metadata, and buffer. (y/n): "
            ).strip().lower()
            if confirm == "y":
                sql_tables  = metadata_manager.get_sql_tables()
                mongo_colls = metadata_manager.get_mongo_collections()
                crud_mgr.reset_database(sql_tables, mongo_colls)
                metadata_manager.reset()

                # Remove SQLite file if using a file-based URL
                if config.SQL_URL.startswith("sqlite:///"):
                    db_path = Path(config.SQL_URL.replace("sqlite:///", ""))
                    if db_path.exists():
                        try:
                            os.remove(db_path)
                        except Exception as e:
                            print(f"Warning: Could not delete database file: {e}")

                crud_mgr   = startup_checks()
                buffer_mgr = buffer_manager.BufferManager(crud_mgr)
                q_engine   = query_engine.QueryEngine(crud_mgr, buffer_mgr)
                print("All data cleared. Schema, buffer, and database reset.")

        elif choice == "0":
            print("Goodbye.")
            sys.exit(0)


if __name__ == "__main__":
    print_header()
    main_menu()
