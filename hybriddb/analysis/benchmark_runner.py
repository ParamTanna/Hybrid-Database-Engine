"""
benchmark_runner.py  —  Performance Evaluation Tool
===================================================

Runs benchmarks comparing Hybrid Framework vs Direct Database Access.
Generates charts, tables, and summary reports.

Usage:
    python benchmark_runner.py

Output:
    - benchmark_results.json
    - benchmark_report.md
    - charts/latency_comparison.png
    - charts/data_distribution.png
    - charts/throughput_under_load.png
    - charts/coordination_overhead.png
"""

import json
import os
import shutil
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List

try:
    import matplotlib.pyplot as plt

    plt_available = True
except ImportError:
    plt_available = False
    print("Warning: matplotlib not installed. Charts will not be generated.")
    print("Install with: pip install matplotlib")

from pymongo import MongoClient

from hybriddb.config import paths
from hybriddb.core import sql_db
from hybriddb.core.transaction_coordinator import TransactionCoordinator
from hybriddb.crud.read_operation import execute_read
from hybriddb.ingestion.classification import _main_table_name

# ============================================================================
# Constants
# ============================================================================

METADATA_FILE = paths.METADATA_FILE
METADATA_BACKUP = paths.METADATA_BACKUP
MONGO_URI = paths.MONGO_URI
MONGO_DB_NAME = paths.MONGO_DB_NAME
CHARTS_DIR = str(paths.CHARTS_DIR)
RESULTS_FILE = paths.RESULTS_FILE
REPORT_FILE = paths.BENCHMARK_REPORT


# ============================================================================
# Benchmark Safety: Metadata Freeze
# ============================================================================

@contextmanager
def frozen_metadata():
    """
    Context manager that:
      1. Backs up metadata_store.json before benchmark
      2. Yields (lets benchmark run)
      3. Restores metadata_store.json after benchmark
         regardless of success or failure

    This prevents benchmark inserts/deletes from triggering
    reclassification and permanently migrating your real schema.
    """
    backed_up = False
    try:
        if os.path.exists(METADATA_FILE):
            shutil.copy2(METADATA_FILE, METADATA_BACKUP)
            backed_up = True
            print(f"[SAFETY] Metadata frozen — backup saved to {METADATA_BACKUP}")
        yield
    finally:
        if backed_up and os.path.exists(METADATA_BACKUP):
            shutil.copy2(METADATA_BACKUP, METADATA_FILE)
            os.remove(METADATA_BACKUP)
            print(f"[SAFETY] Metadata restored from backup")
        elif not backed_up:
            print(f"[SAFETY] Warning: no backup was made")


# ============================================================================
# Utility Functions
# ============================================================================

def setup_charts_dir():
    if not os.path.exists(CHARTS_DIR):
        os.makedirs(CHARTS_DIR)


def load_metadata() -> dict:
    try:
        with open(METADATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        raise FileNotFoundError(
            f"{METADATA_FILE} not found. Run schema registration first."
        )


def get_sql_connection():
    return sql_db.dict_connect(autocommit=True)


def get_mongo_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)


def _repeat(fn, n: int) -> List[float]:
    """Run fn() n times, return list of durations in ms."""
    times = []
    for _ in range(n):
        start = time.perf_counter()
        fn()
        times.append((time.perf_counter() - start) * 1000)
    return times


def _avg(times: List[float]) -> float:
    return sum(times) / len(times) if times else 0.0


def _get_main_sql_table(meta: dict) -> str | None:
    """Return the main SQL table name derived from global_key."""
    global_key = meta.get("global_key")
    if not global_key:
        return None
    return _main_table_name(global_key)


def _get_main_mongo_collection(meta: dict) -> str | None:
    """Return the main MongoDB collection name derived from global_key."""
    global_key = meta.get("global_key")
    if not global_key:
        return None
    return _main_table_name(global_key)


def _get_sample_pk(meta: dict):
    """
    Find one real primary key value from the SQL main table.
    Returns None if the table is empty or unreachable.
    """
    table = _get_main_sql_table(meta)
    if not table:
        return None
    global_key = meta.get("global_key")
    try:
        conn = get_sql_connection()
        with conn.cursor() as cur:
            cur.execute(f"SELECT {global_key} FROM {table} LIMIT 1")
            row = cur.fetchone()
        conn.close()
        return row[global_key] if row else None
    except Exception:
        return None


def _build_test_record(pk_val: int, meta: dict) -> dict:
    """
    Build a minimal valid test record that will pass framework validation.

    Rules:
      - Include global_key always
      - Include ALL not_null scalar fields regardless of backend
        (SQL, Mongo, Buffer — validation checks all of them)
      - Skip array/object fields (they need nested structure)
      - Use type-appropriate placeholder values
    """
    global_key = meta.get("global_key", "customer_id")
    fields = meta.get("fields", {})

    record = {global_key: pk_val}

    for fname, fmeta in fields.items():
        # Only top-level fields
        if fmeta.get("level") != 0:
            continue
        # Already set
        if fname == global_key:
            continue
        # Skip arrays and objects — too complex for a minimal record
        ftype = fmeta.get("type", "string")
        if ftype in ("array", "object"):
            continue

        # Always include not_null fields (others optional)
        if not fmeta.get("not_null", False):
            continue

        # Generate a type-safe placeholder
        if ftype == "int":
            record[fname] = pk_val
        elif ftype == "float":
            record[fname] = float(pk_val)
        elif ftype == "boolean":
            record[fname] = True
        else:
            record[fname] = f"bench_{fname}_{pk_val}"

    return record


# ============================================================================
# Safe operation wrappers (skip reclassify)
# ============================================================================

def _safe_insert(tc: TransactionCoordinator, record: dict) -> dict:
    """Insert test record with reclassification disabled."""
    query = {
        "operation": "insert",
        "data": record,
        "_skip_reclassify": True,
    }
    return tc.execute(query)


def _safe_delete(tc: TransactionCoordinator, global_key: str, pk_val) -> dict:
    """Delete test record."""
    return tc.execute({
        "operation": "delete",
        "where": {global_key: pk_val},
    })


def _safe_update(
    tc: TransactionCoordinator,
    global_key: str,
    pk_val,
    update_field: str,
    new_val,
) -> dict:
    """Update test record with reclassification disabled."""
    return tc.execute({
        "operation": "update",
        "where": {global_key: pk_val},
        "data": {update_field: new_val},
        "_skip_reclassify": True,
    })


# ============================================================================
# 1. Metadata Lookup Overhead
# ============================================================================

def benchmark_metadata_overhead(repeats: int = 50) -> Dict[str, float]:
    print("\n[BENCHMARK] Metadata Lookup Overhead...")

    results: Dict[str, float] = {}

    # (a) disk load time
    def _load():
        with open(METADATA_FILE, "r", encoding="utf-8") as f:
            json.load(f)

    load_times = _repeat(_load, repeats)
    results["metadata_load_ms"] = _avg(load_times)
    print(f"    metadata_store.json load:  {results['metadata_load_ms']:.3f} ms")

    # (b) single field classification lookup
    meta = load_metadata()
    fields = meta.get("fields", {})
    sample_field = next(iter(fields), None)

    if sample_field:
        def _lookup():
            _ = fields.get(sample_field, {}).get("storage_backend", "Buffer")

        lookup_times = _repeat(_lookup, repeats * 10)
        results["field_lookup_ms"] = _avg(lookup_times)
        print(f"    Single field lookup:       {results['field_lookup_ms']:.6f} ms")
    else:
        results["field_lookup_ms"] = 0.0

    # (c) wildcard expand
    def _expand():
        return [
            fname for fname, fdata in fields.items()
            if fdata.get("parent") is None
        ]

    expand_times = _repeat(_expand, repeats)
    results["wildcard_expand_ms"] = _avg(expand_times)
    print(f"    Wildcard field expansion:  {results['wildcard_expand_ms']:.4f} ms")

    return results


# ============================================================================
# 2. Transaction Coordination Overhead
# ============================================================================

def benchmark_coordination_overhead(meta: dict) -> Dict[str, float]:
    print("\n[BENCHMARK] Transaction Coordination Overhead...")

    results: Dict[str, float] = {}
    tc = TransactionCoordinator()
    global_key = meta.get("global_key", "customer_id")
    main_table = _get_main_sql_table(meta)
    main_collection = _get_main_mongo_collection(meta)

    # ── Framework insert ──────────────────────────────────────────────────
    framework_times = []
    test_pks = list(range(70001, 70011))
    for pk in test_pks:
        _safe_delete(tc, global_key, pk)  # ensure clean slate
        record = _build_test_record(pk, meta)
        start = time.perf_counter()
        result = _safe_insert(tc, record)
        duration_ms = (time.perf_counter() - start) * 1000
        if result.get("success"):
            framework_times.append(duration_ms)
        _safe_delete(tc, global_key, pk)

    results["framework_insert_ms"] = _avg(framework_times)
    print(f"    Framework insert (avg):    {results['framework_insert_ms']:.2f} ms")

    # ── Direct SQL insert ─────────────────────────────────────────────────
    if main_table:
        direct_sql_times = []
        conn = get_sql_connection()
        # Restrict the direct-insert benchmark to the main table's own columns so
        # the INSERT satisfies (or simply does not exercise) foreign keys; no
        # orphan-insert trick is needed under PostgreSQL.
        table_col_names = set(sql_db.column_names(conn, main_table))

        for i, pk in enumerate(test_pks):
            record = _build_test_record(pk, meta)
            row = {k: v for k, v in record.items() if k in table_col_names}

            start = time.perf_counter()
            with conn.cursor() as cur:
                sql_db.upsert(cur, main_table, row, [global_key], update=True)
            direct_sql_times.append((time.perf_counter() - start) * 1000)

            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {main_table} WHERE {global_key} = %s", (pk,)
                )

        conn.close()
        results["direct_sql_insert_ms"] = _avg(direct_sql_times)
        print(f"    Direct SQL insert (avg):   {results['direct_sql_insert_ms']:.2f} ms")
    else:
        results["direct_sql_insert_ms"] = 0.0

    # ── Direct Mongo insert ───────────────────────────────────────────────
    if main_collection:
        direct_mongo_times = []
        try:
            client = get_mongo_client()
            db = client[MONGO_DB_NAME]
            collection = db[main_collection]

            for pk in test_pks:
                doc = {global_key: pk, "_bench": True}
                start = time.perf_counter()
                collection.replace_one({global_key: pk}, doc, upsert=True)
                direct_mongo_times.append((time.perf_counter() - start) * 1000)
                collection.delete_one({global_key: pk})

            client.close()
            results["direct_mongo_insert_ms"] = _avg(direct_mongo_times)
            print(f"    Direct Mongo insert (avg): {results['direct_mongo_insert_ms']:.2f} ms")
        except Exception as e:
            print(f"    Direct Mongo insert skipped: {e}")
            results["direct_mongo_insert_ms"] = 0.0
    else:
        results["direct_mongo_insert_ms"] = 0.0

    # ── Compute overhead ──────────────────────────────────────────────────
    direct_combined = results["direct_sql_insert_ms"] + results["direct_mongo_insert_ms"]
    overhead = results["framework_insert_ms"] - direct_combined
    overhead_pct = (overhead / direct_combined * 100) if direct_combined > 0 else 0.0
    results["coordination_overhead_ms"] = max(overhead, 0.0)
    results["coordination_overhead_pct"] = round(overhead_pct, 1)

    print(f"    Direct combined (avg):     {direct_combined:.2f} ms")
    print(f"    Coordination overhead:     {results['coordination_overhead_ms']:.2f} ms  "
          f"({results['coordination_overhead_pct']}%)")

    return results


# ============================================================================
# 3. Data Ingestion Latency
# ============================================================================

def benchmark_data_ingestion(meta: dict, num_records: int = 20) -> float:
    print("\n[BENCHMARK] Data Ingestion Latency...")

    tc = TransactionCoordinator()
    global_key = meta.get("global_key", "customer_id")

    times = []
    inserted_pks = []

    for i in range(num_records):
        pk = 90000 + i
        record = _build_test_record(pk, meta)
        start = time.perf_counter()
        result = _safe_insert(tc, record)
        duration_ms = (time.perf_counter() - start) * 1000
        if result.get("success"):
            times.append(duration_ms)
            inserted_pks.append(pk)

    # Cleanup
    for pk in inserted_pks:
        _safe_delete(tc, global_key, pk)

    avg_time = _avg(times)
    print(f"    Inserted {len(times)}/{num_records} records successfully")
    print(f"    Avg ingestion time: {avg_time:.2f} ms per record")
    return avg_time


# ============================================================================
# 4. Logical Query Response Time
# ============================================================================

def benchmark_logical_query(meta: dict) -> Dict[str, float]:
    print("\n[BENCHMARK] Logical Query Response Time (via Framework)...")

    global_key = meta.get("global_key", "customer_id")
    sample_pk = _get_sample_pk(meta)
    results: Dict[str, float] = {}

    # Single record by PK
    if sample_pk is not None:
        def _single():
            execute_read(
                {"operation": "read", "fields": ["*"], "where": {global_key: sample_pk}},
                meta,
            )

        times = _repeat(_single, 10)
        results["single_record_by_pk"] = _avg(times)
        print(f"    Single record by PK:   {results['single_record_by_pk']:.2f} ms")
    else:
        results["single_record_by_pk"] = 0.0

    # All records
    def _all():
        execute_read({"operation": "read", "fields": ["*"], "where": {}}, meta)

    times = _repeat(_all, 5)
    results["all_records"] = _avg(times)
    print(f"    All records:           {results['all_records']:.2f} ms")

    # Specific scalar fields only
    scalar_fields = [
        fname for fname, fmeta in meta.get("fields", {}).items()
        if fmeta.get("level") == 0 and fmeta.get("type") not in ("array", "object")
    ][:3]
    if not scalar_fields:
        scalar_fields = [global_key]

    def _specific():
        execute_read(
            {"operation": "read", "fields": scalar_fields, "where": {}}, meta
        )

    times = _repeat(_specific, 5)
    results["specific_fields"] = _avg(times)
    print(f"    Specific fields only:  {results['specific_fields']:.2f} ms")

    return results


# ============================================================================
# 5. Direct SQL Query Time (with improved update target detection)
# ============================================================================

def benchmark_direct_sql(meta: dict) -> Dict[str, float]:
    print("\n[BENCHMARK] Direct SQL Query Time (bypassing framework)...")

    results: Dict[str, float] = {}
    global_key = meta.get("global_key", "customer_id")
    main_table = _get_main_sql_table(meta)
    sample_pk = _get_sample_pk(meta)

    if not main_table:
        print("    No main SQL table found — skipping")
        return results

    conn = get_sql_connection()

    # ── Single record by PK ───────────────────────────────────────────────
    if sample_pk is not None:
        def _single():
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {main_table} WHERE {global_key} = %s",
                    (sample_pk,),
                )
                cur.fetchall()

        times = _repeat(_single, 10)
        results["single_record_by_pk"] = _avg(times)
        print(f"    Single record by PK : {results['single_record_by_pk']:.3f} ms")
    else:
        results["single_record_by_pk"] = 0.0

    # ── All records ───────────────────────────────────────────────────────
    def _all():
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {main_table}")
            cur.fetchall()

    times = _repeat(_all, 5)
    results["all_records"] = _avg(times)
    print(f"    All records         : {results['all_records']:.3f} ms")

    # ── Update latency (find any updatable column across ALL SQL tables) ──
    km_sql = meta.get("key_management", {}).get("SQL", {})
    update_table = None
    update_col = None

    # Priority: main table first, then other tables
    tables_to_check = [main_table] + [t for t in km_sql if t != main_table]

    for table_name in tables_to_check:
        if table_name not in km_sql:
            continue
        # Get column info
        col_names = sql_db.column_names(conn, table_name)
        non_pk = [c for c in col_names if c != global_key]
        if not non_pk:
            continue
        # Check table has rows with our sample_pk (or any row)
        with conn.cursor() as cur:
            if sample_pk:
                cur.execute(
                    f"SELECT COUNT(*) FROM {table_name} WHERE {global_key} = %s",
                    (sample_pk,),
                )
            else:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()["count"]
        if count > 0:
            update_table = table_name
            update_col = non_pk[0]
            break

    if update_table and update_col and sample_pk is not None:
        def _update():
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {update_table} SET {update_col} = {update_col} "
                    f"WHERE {global_key} = %s",
                    (sample_pk,),
                )

        times = _repeat(_update, 10)
        results["update_latency"] = _avg(times)
        print(f"    Update ({update_table}.{update_col}) : {results['update_latency']:.3f} ms")
    else:
        print("    ⚠️  No updatable SQL column found across any table — update benchmark skipped")
        results["update_latency"] = 0.0

    conn.close()
    return results


# ============================================================================
# 6. Direct MongoDB Query Time (with update latency)
# ============================================================================

def benchmark_direct_mongo(meta: dict) -> Dict[str, float]:
    print("\n[BENCHMARK] Direct MongoDB Query Time (bypassing framework)...")

    results: Dict[str, float] = {}
    global_key = meta.get("global_key", "customer_id")
    main_collection = _get_main_mongo_collection(meta)
    sample_pk = _get_sample_pk(meta)

    try:
        client = get_mongo_client()
        client.admin.command("ping")
    except Exception as e:
        print(f"    MongoDB unreachable — skipping: {e}")
        return results

    db = client[MONGO_DB_NAME]
    collection = db[main_collection]

    # Single doc by PK
    if sample_pk is not None:
        def _single():
            list(collection.find({global_key: sample_pk}, {"_id": 0}))

        times = _repeat(_single, 10)
        results["single_doc_by_pk"] = _avg(times)
        print(f"    Single doc by PK   : {results['single_doc_by_pk']:.3f} ms")
    else:
        results["single_doc_by_pk"] = 0.0

    # All docs
    def _all():
        list(collection.find({}, {"_id": 0}))

    times = _repeat(_all, 5)
    results["all_docs"] = _avg(times)
    print(f"    All docs           : {results['all_docs']:.3f} ms")

    # Update latency
    if sample_pk is not None:
        def _update():
            collection.update_one(
                {global_key: sample_pk},
                {"$set": {"_bench_ts": time.time()}},
            )

        times = _repeat(_update, 10)
        results["update_latency"] = _avg(times)
        print(f"    Update (direct)    : {results['update_latency']:.3f} ms")

        # Cleanup bench field
        collection.update_one(
            {global_key: sample_pk},
            {"$unset": {"_bench_ts": ""}},
        )
    else:
        results["update_latency"] = 0.0

    client.close()
    return results


# ============================================================================
# 7. Framework Update Latency
# ============================================================================

def benchmark_framework_update(meta: dict) -> Dict[str, float]:
    print("\n[BENCHMARK] Framework Update Latency...")

    results: Dict[str, float] = {}
    tc = TransactionCoordinator()
    global_key = meta.get("global_key", "customer_id")
    sample_pk = _get_sample_pk(meta)

    if sample_pk is None:
        print("    No sample record found — skipping")
        results["update_latency"] = 0.0
        return results

    fields = meta.get("fields", {})
    update_field = None
    for fname, fmeta in fields.items():
        if (
            fname != global_key
            and fmeta.get("level") == 0
            and fmeta.get("type") not in ("array", "object")
        ):
            update_field = fname
            break

    if update_field is None:
        print("    No updatable scalar field found — skipping")
        results["update_latency"] = 0.0
        return results

    # Read original value to restore later
    original_records = execute_read(
        {"operation": "read", "fields": [update_field], "where": {global_key: sample_pk}},
        meta,
    )
    original_value = original_records[0].get(update_field) if original_records else None

    times = []
    for i in range(10):
        ftype = fields[update_field].get("type", "string")
        if ftype == "int":
            new_val = sample_pk + i
        elif ftype == "float":
            new_val = float(sample_pk + i)
        elif ftype == "boolean":
            new_val = (i % 2 == 0)
        else:
            new_val = f"bench_upd_{i}"

        start = time.perf_counter()
        result = _safe_update(tc, global_key, sample_pk, update_field, new_val)
        duration_ms = (time.perf_counter() - start) * 1000
        if result.get("success"):
            times.append(duration_ms)

    # Restore original value
    if original_value is not None:
        _safe_update(tc, global_key, sample_pk, update_field, original_value)

    results["update_latency"] = _avg(times)
    print(f"    Framework update (avg): {results['update_latency']:.2f} ms  "
          f"(over {len(times)} runs)")
    return results


# ============================================================================
# 8. Throughput Under Increasing Load
# ============================================================================

def benchmark_throughput_under_load(meta: dict) -> Dict[str, Any]:
    print("\n[BENCHMARK] Throughput Under Increasing Load...")

    results: Dict[str, Any] = {}
    tc = TransactionCoordinator()
    global_key = meta.get("global_key", "customer_id")

    # Read throughput curve
    batch_sizes = [10, 25, 50, 100, 200]
    read_ops_per_sec: List[int] = []

    for batch in batch_sizes:
        def _read_once():
            execute_read({"operation": "read", "fields": ["*"], "where": {}}, meta)

        start = time.perf_counter()
        for _ in range(batch):
            _read_once()
        elapsed = time.perf_counter() - start
        ops = int(batch / elapsed) if elapsed > 0 else 0
        read_ops_per_sec.append(ops)
        print(f"    {batch:>4} reads  → {ops:>5} ops/sec")

    results["read_load_curve"] = {
        "batch_sizes": batch_sizes,
        "ops_per_sec": read_ops_per_sec,
    }

    # Write throughput (fixed batch)
    write_batch = 10
    write_pks = [80000 + i for i in range(write_batch)]

    # Clean slate
    for pk in write_pks:
        _safe_delete(tc, global_key, pk)

    start = time.perf_counter()
    inserted = 0
    for pk in write_pks:
        r = _safe_insert(tc, _build_test_record(pk, meta))
        if r.get("success"):
            inserted += 1
    elapsed = time.perf_counter() - start
    writes_per_sec = int(inserted / elapsed) if elapsed > 0 else 0

    # Cleanup
    for pk in write_pks:
        _safe_delete(tc, global_key, pk)

    results["writes_per_second"] = writes_per_sec
    results["reads_per_second"] = read_ops_per_sec[-1] if read_ops_per_sec else 0
    print(f"    Write throughput:     {writes_per_sec} ops/sec  ({inserted}/{write_batch} succeeded)")

    return results


# ============================================================================
# 9. Data Distribution
# ============================================================================

def benchmark_data_distribution(meta: dict) -> Dict[str, int]:
    print("\n[BENCHMARK] Data Distribution Analysis...")

    fields = meta.get("fields", {})
    distribution = {
        "SQL": 0,
        "Mongo_Embedded": 0,
        "Mongo_Reference": 0,
        "Buffer": 0,
    }

    for fname, fmeta in fields.items():
        if fmeta.get("level") != 0:
            continue
        backend = fmeta.get("storage_backend", "Buffer")
        detail = fmeta.get("storage_detail", "")

        if backend == "SQL":
            distribution["SQL"] += 1
        elif backend == "Mongo":
            if "reference" in detail.lower():
                distribution["Mongo_Reference"] += 1
            else:
                distribution["Mongo_Embedded"] += 1
        else:
            distribution["Buffer"] += 1

    total = sum(distribution.values())
    print(f"    Total top-level fields: {total}")
    for backend, count in distribution.items():
        pct = (count / total * 100) if total > 0 else 0
        print(f"    {backend:<20}: {count}  ({pct:.1f}%)")

    return distribution


# ============================================================================
# 10. Chart Generation
# ============================================================================

def generate_charts(results: Dict):
    if not plt_available:
        print("[WARNING] Cannot generate charts — matplotlib not installed")
        return

    print("\n[INFO] Generating charts...")
    setup_charts_dir()

    # Chart 1: Latency Comparison (bar)
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Single record read
    ax = axes[0]
    labels = ["Framework", "Direct SQL", "Direct Mongo"]
    values = [
        results.get("logical_query", {}).get("single_record_by_pk", 0),
        results.get("direct_sql", {}).get("single_record_by_pk", 0),
        results.get("direct_mongo", {}).get("single_doc_by_pk", 0),
    ]
    colors = ["#0f5e9c", "#2e7d32", "#f57c00"]
    bars = ax.bar(labels, values, color=colors, edgecolor="black", width=0.5)
    ax.set_ylabel("Time (ms)")
    ax.set_title("Single Record Read Latency")
    ax.grid(axis="y", alpha=0.3)
    for bar, val in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.02,
            f"{val:.2f} ms",
            ha="center", va="bottom", fontsize=9,
        )

    # Update latency
    ax2 = axes[1]
    update_labels = ["Framework", "Direct SQL", "Direct Mongo"]
    update_values = [
        results.get("framework_update", {}).get("update_latency", 0),
        results.get("direct_sql", {}).get("update_latency", 0),
        results.get("direct_mongo", {}).get("update_latency", 0),
    ]
    bars2 = ax2.bar(update_labels, update_values, color=colors, edgecolor="black", width=0.5)
    ax2.set_ylabel("Time (ms)")
    ax2.set_title("Update Latency Comparison")
    ax2.grid(axis="y", alpha=0.3)
    for bar, val in zip(bars2, update_values):
        ax2.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.02,
            f"{val:.2f} ms",
            ha="center", va="bottom", fontsize=9,
        )

    plt.tight_layout()
    path = os.path.join(CHARTS_DIR, "latency_comparison.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"    Saved: {path}")

    # Chart 2: Throughput Under Load (line)
    load_curve = results.get("throughput", {}).get("read_load_curve", {})
    batch_sizes = load_curve.get("batch_sizes", [])
    ops_per_sec = load_curve.get("ops_per_sec", [])

    if batch_sizes and ops_per_sec:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.plot(
            batch_sizes, ops_per_sec,
            marker="o", linewidth=2, markersize=8, color="#0f5e9c",
            label="Framework reads/sec",
        )
        ax.set_xlabel("Number of Concurrent Read Operations")
        ax.set_ylabel("Operations per Second")
        ax.set_title("Read Throughput Under Increasing Load")
        ax.grid(alpha=0.3)
        ax.legend()
        for x, y in zip(batch_sizes, ops_per_sec):
            ax.annotate(
                str(y), xy=(x, y), xytext=(0, 8),
                textcoords="offset points", ha="center", fontsize=9,
            )
        plt.tight_layout()
        path = os.path.join(CHARTS_DIR, "throughput_under_load.png")
        plt.savefig(path, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"    Saved: {path}")

    # Chart 3: Data Distribution (pie)
    dist = results.get("distribution", {})
    non_zero = {k: v for k, v in dist.items() if v > 0}
    if non_zero:
        fig, ax = plt.subplots(figsize=(7, 7))
        pie_colors = ["#0f5e9c", "#2e7d32", "#f57c00", "#9e9e9e"]
        explode = [0.05] * len(non_zero)
        ax.pie(
            list(non_zero.values()),
            labels=list(non_zero.keys()),
            autopct="%1.1f%%",
            colors=pie_colors[: len(non_zero)],
            explode=explode,
            shadow=True,
        )
        ax.set_title("Field Distribution Across Storage Backends")
        path = os.path.join(CHARTS_DIR, "data_distribution.png")
        plt.savefig(path, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"    Saved: {path}")

    # Chart 4: Coordination Overhead Breakdown
    coord = results.get("coordination_overhead", {})
    if coord:
        fig, ax = plt.subplots(figsize=(8, 5))
        comp_labels = ["Direct SQL", "Direct Mongo", "Combined Direct", "Framework Total"]
        comp_values = [
            coord.get("direct_sql_insert_ms", 0),
            coord.get("direct_mongo_insert_ms", 0),
            coord.get("direct_sql_insert_ms", 0) + coord.get("direct_mongo_insert_ms", 0),
            coord.get("framework_insert_ms", 0),
        ]
        bar_colors = ["#2e7d32", "#f57c00", "#7b1fa2", "#0f5e9c"]
        bars = ax.bar(comp_labels, comp_values, color=bar_colors, edgecolor="black", width=0.5)
        ax.set_ylabel("Time (ms)")
        ax.set_title("Coordination Overhead Breakdown (Insert)")
        ax.grid(axis="y", alpha=0.3)
        for bar, val in zip(bars, comp_values):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.05,
                f"{val:.2f} ms",
                ha="center", va="bottom", fontsize=9,
            )
        plt.tight_layout()
        path = os.path.join(CHARTS_DIR, "coordination_overhead.png")
        plt.savefig(path, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"    Saved: {path}")


# ============================================================================
# 11. Markdown Report
# ============================================================================

def generate_markdown_report(results: Dict, timestamp: str):
    dist = results.get("distribution", {})
    dist_total = max(sum(dist.values()), 1)

    def _pct(key):
        return round(dist.get(key, 0) / dist_total * 100, 1)

    coord = results.get("coordination_overhead", {})
    meta_oh = results.get("metadata_overhead", {})
    lq = results.get("logical_query", {})
    dsql = results.get("direct_sql", {})
    dmongo = results.get("direct_mongo", {})
    fw_upd = results.get("framework_update", {})
    tp = results.get("throughput", {})
    load_curve = tp.get("read_load_curve", {})

    report = f"""# Hybrid Database Framework — Performance Evaluation Report

**Generated:** {timestamp}

---

## 1. Metadata Lookup Overhead

| Operation | Avg Time |
|-----------|----------|
| Load `metadata_store.json` from disk | `{meta_oh.get('metadata_load_ms', 0):.3f}` ms |
| Single field classification lookup | `{meta_oh.get('field_lookup_ms', 0):.6f}` ms |
| Wildcard field expansion | `{meta_oh.get('wildcard_expand_ms', 0):.4f}` ms |

---

## 2. Transaction Coordination Overhead

| Component | Avg Time (ms) |
|-----------|--------------|
| Direct SQL insert | `{coord.get('direct_sql_insert_ms', 0):.2f}` |
| Direct MongoDB insert | `{coord.get('direct_mongo_insert_ms', 0):.2f}` |
| Combined direct (SQL + Mongo) | `{coord.get('direct_sql_insert_ms', 0) + coord.get('direct_mongo_insert_ms', 0):.2f}` |
| Framework coordinated insert | `{coord.get('framework_insert_ms', 0):.2f}` |
| **Coordination overhead** | **`{coord.get('coordination_overhead_ms', 0):.2f}`** (`{coord.get('coordination_overhead_pct', 0)}`%) |

---

## 3. Data Ingestion Latency

| Metric | Value |
|--------|-------|
| Avg ingestion time per record | `{results.get('ingestion_latency', 0):.2f}` ms |

---

## 4. Query Response Time — Framework vs Direct Access

### 4a. Single Record Read

| Method | Latency (ms) |
|--------|-------------|
| Framework | `{lq.get('single_record_by_pk', 0):.2f}` |
| Direct SQL | `{dsql.get('single_record_by_pk', 0):.2f}` |
| Direct MongoDB | `{dmongo.get('single_doc_by_pk', 0):.2f}` |

### 4b. All Records Read

| Method | Latency (ms) |
|--------|-------------|
| Framework | `{lq.get('all_records', 0):.2f}` |
| Direct SQL | `{dsql.get('all_records', 0):.2f}` |
| Direct MongoDB | `{dmongo.get('all_docs', 0):.2f}` |

### 4c. Update Latency

| Method | Latency (ms) |
|--------|-------------|
| Framework | `{fw_upd.get('update_latency', 0):.2f}` |
| Direct SQL | `{dsql.get('update_latency', 0):.2f}` |
| Direct MongoDB | `{dmongo.get('update_latency', 0):.2f}` |

---

## 5. System Throughput

### 5a. Read Throughput Under Load

| Batch Size | Ops / sec |
|-----------|-----------|
{chr(10).join(f"| {b} | {o} |" for b, o in zip(load_curve.get('batch_sizes', []), load_curve.get('ops_per_sec', [])))}

### 5b. Write Throughput

| Metric | Value |
|--------|-------|
| Framework writes/sec | `{tp.get('writes_per_second', 0)}` |

---

## 6. Data Distribution

| Backend | Field Count | Percentage |
|---------|-------------|------------|
| SQL | `{dist.get('SQL', 0)}` | `{_pct('SQL')}%` |
| MongoDB (Embedded) | `{dist.get('Mongo_Embedded', 0)}` | `{_pct('Mongo_Embedded')}%` |
| MongoDB (Reference) | `{dist.get('Mongo_Reference', 0)}` | `{_pct('Mongo_Reference')}%` |
| Buffer | `{dist.get('Buffer', 0)}` | `{_pct('Buffer')}%` |

---

## 7. Key Observations

1. **Abstraction overhead is measurable but justified**: The framework adds
   `{coord.get('coordination_overhead_ms', 0):.1f}` ms per insert for atomic cross-backend coordination.

2. **Metadata load cost is amortised**: At `{meta_oh.get('metadata_load_ms', 0):.2f}` ms per load,
   frequent requests should cache the metadata dict in memory.

3. **Read overhead is dominated by cross-backend merge**: A single logical read
   must hit SQLite, MongoDB, and the buffer, merge results, and coerce types.

4. **Update overhead is highest**: The framework implements update as
   delete-then-insert with full snapshot capture for atomicity across two databases.

5. **Throughput scales sub-linearly under load**: As batch size grows,
   per-operation cost rises due to the growing result-merge step.

---

## 8. Where Abstraction Introduces Overhead

- Every operation loads and parses `metadata_store.json`
- Insert performs duplicate-check read before writing
- Update captures pre-state snapshot before delete+re-insert
- All reads merge results from up to three backends

## 9. Where Abstraction Simplifies Development

- Single unified API for insert/read/update/delete regardless of backend
- Schema-driven field routing — adding a new backend requires only metadata change
- Automatic rollback on partial failure
- Query written once, executed across SQL and MongoDB transparently

---

## 10. Limitations

- Tests run on local development machine (single process, no network latency)
- Dataset size is small (prototype scale)
- No concurrent client load testing
- Reclassification cost not included in benchmarks (frozen during tests)

---

**Charts generated:**
- `charts/latency_comparison.png`
- `charts/throughput_under_load.png`
- `charts/data_distribution.png`
- `charts/coordination_overhead.png`
"""

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"\n[INFO] Report saved to {REPORT_FILE}")


# ============================================================================
# Main Runner
# ============================================================================

def run_benchmarks():
    print("=" * 70)
    print("HYBRID DATABASE FRAMEWORK — PERFORMANCE BENCHMARKING")
    print("=" * 70)

    paths.ensure_dirs()
    setup_charts_dir()
    timestamp = datetime.now(timezone.utc).isoformat()

    results: Dict[str, Any] = {
        "timestamp": timestamp,
        "version": "2.1",
    }

    with frozen_metadata():
        try:
            meta = load_metadata()
            results["metadata_loaded"] = True

            results["metadata_overhead"] = benchmark_metadata_overhead()
            results["coordination_overhead"] = benchmark_coordination_overhead(meta)
            results["ingestion_latency"] = benchmark_data_ingestion(meta)
            results["logical_query"] = benchmark_logical_query(meta)
            results["direct_sql"] = benchmark_direct_sql(meta)
            results["direct_mongo"] = benchmark_direct_mongo(meta)
            results["framework_update"] = benchmark_framework_update(meta)
            results["throughput"] = benchmark_throughput_under_load(meta)
            results["distribution"] = benchmark_data_distribution(meta)

            results["status"] = "SUCCESS"

        except Exception as e:
            results["status"] = "FAILED"
            results["error"] = str(e)
            print(f"\n[BENCHMARK FAILED] {e}")
            import traceback
            traceback.print_exc()

    # Save results
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n[INFO] Raw results saved to {RESULTS_FILE}")

    generate_charts(results)
    generate_markdown_report(results, timestamp)

    print("\n" + "=" * 70)
    print("BENCHMARK COMPLETE")
    print("=" * 70)
    print(f"\nFiles written:")
    print(f"  {RESULTS_FILE}")
    print(f"  {REPORT_FILE}")
    print(f"  charts/latency_comparison.png")
    print(f"  charts/throughput_under_load.png")
    print(f"  charts/data_distribution.png")
    print(f"  charts/coordination_overhead.png")

    return results


if __name__ == "__main__":
    run_benchmarks()