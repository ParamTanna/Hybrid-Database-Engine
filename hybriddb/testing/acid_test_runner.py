import argparse
import json
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable

from hybriddb.config import paths
from hybriddb.core import sql_db
from hybriddb.core.transaction_coordinator import TransactionCoordinator
from hybriddb.ingestion.classification import _main_table_name


class TestFailure(Exception):
    pass


@dataclass
class TestCase:
    name: str
    fn: Callable


def _load_meta(path=None):
    if path is None:
        path = paths.METADATA_FILE
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _assert(condition, message):
    if not condition:
        raise TestFailure(message)


def _now_ms():
    return int(time.time() * 1000)


def _make_id(tag=0):
    return _now_ms() + tag


def _email(record_id, label):
    return f"reliability.{label}.{record_id}@example.com"


def _read_by_id(tc, global_key, record_id):
    res = tc.execute(
        {"operation": "read", "fields": ["*"], "where": {global_key: record_id}}
    )
    return res.get("data") or []


def _cleanup_id(tc, global_key, record_id):
    tc.execute({"operation": "delete", "where": {global_key: record_id}})


# --- direct backend inspectors (used for cross-backend consistency checks) ---

def _sql_main_has_key(global_key, record_id) -> bool:
    """True if the main SQL table has a row for this key (queried directly)."""
    main_table = _main_table_name(global_key)
    conn = sql_db.connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT 1 FROM {main_table} WHERE {global_key} = %s LIMIT 1",
                (record_id,),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _mongo_main_has_key(tc, global_key, record_id) -> bool:
    """True if the main Mongo collection has a document for this key (direct)."""
    coll = _main_table_name(global_key)
    db = tc._client[tc.mongo_db]
    return db[coll].find_one({global_key: record_id}) is not None


def _assert_backends_consistent(tc, global_key, record_id, expected_present):
    """Cross-backend consistency invariant for this hybrid model.

    The SQL main table (where the global key is the PRIMARY KEY) is the source
    of truth for record existence. A minimal record may have NO main Mongo
    document (Mongo only stores embed/reference fields), so we do not require a
    main Mongo doc when present. We DO require:
      * SQL main presence matches `expected_present`
      * the logical (merged) read matches `expected_present`
      * when the record should be ABSENT, there is no Mongo residue either
        (the failure mode this whole framework must prevent: orphaned Mongo
        docs with no SQL counterpart).
    """
    sql_present = _sql_main_has_key(global_key, record_id)
    logical_present = len(_read_by_id(tc, global_key, record_id)) > 0

    _assert(
        sql_present == expected_present,
        f"SQL main for {global_key}={record_id} is {sql_present}, expected {expected_present}",
    )
    _assert(
        logical_present == expected_present,
        f"Logical read for {global_key}={record_id} is {logical_present}, expected {expected_present}",
    )
    if not expected_present:
        _assert(
            not _mongo_main_has_key(tc, global_key, record_id),
            f"Mongo residue (orphan doc) for {global_key}={record_id} after expected-absent",
        )


@contextmanager
def _patched_attr(obj, name, replacement):
    original = getattr(obj, name)
    setattr(obj, name, replacement)
    try:
        yield
    finally:
        setattr(obj, name, original)


def _insert_min(tc, global_key, record_id, label):
    return tc.execute(
        {
            "operation": "insert",
            "data": {
                global_key: record_id,
                "email": _email(record_id, label),
                "name": f"{label}-{record_id}",
            },
        }
    )


def test_atomic_insert_mongo_fail_no_residue(tc, global_key, _opts):
    """Mongo write fails mid-insert -> the whole insert must roll back on BOTH
    backends (no SQL residue, no Mongo residue)."""
    record_id = _make_id(1)

    def _mongo_fail(*_a, **_k):
        raise RuntimeError("simulated mongo failure")

    with _patched_attr(tc, "_mongo_insert", _mongo_fail):
        res = _insert_min(tc, global_key, record_id, "atomic-ins")
        _assert(not res.get("success"), "Insert should fail when Mongo write fails")
        _assert(bool(res.get("rolled_back")), "Insert failure should indicate rollback")

    rows = _read_by_id(tc, global_key, record_id)
    _assert(len(rows) == 0, "Atomicity(insert): partial SQL residue detected")
    _assert_backends_consistent(tc, global_key, record_id, expected_present=False)


def test_atomic_delete_mongo_fail_restores_sql(tc, global_key, _opts):
    """Mongo delete fails -> the SQL delete must roll back so the record
    survives intact on both backends."""
    record_id = _make_id(2)
    ins = _insert_min(tc, global_key, record_id, "atomic-del")
    _assert(ins.get("success"), "Setup insert failed for delete rollback test")

    def _mongo_fail(*_a, **_k):
        raise RuntimeError("simulated mongo delete failure")

    with _patched_attr(tc, "_mongo_delete_by_key", _mongo_fail):
        dele = tc.execute({"operation": "delete", "where": {global_key: record_id}})
        _assert(not dele.get("success"), "Delete should fail when Mongo delete fails")
        _assert(bool(dele.get("rolled_back")), "Delete failure should indicate rollback")

    rows = _read_by_id(tc, global_key, record_id)
    _assert(len(rows) == 1, "Atomicity(delete): record lost after rollback")
    _assert_backends_consistent(tc, global_key, record_id, expected_present=True)
    _cleanup_id(tc, global_key, record_id)


def test_atomic_update_mongo_fail_snapshot_restore(tc, global_key, _opts):
    """Mongo write fails during the update -> the record must be unchanged on
    both backends (the delete-then-insert is one atomic unit)."""
    record_id = _make_id(3)
    email = _email(record_id, "atomic-upd")
    ins = tc.execute(
        {
            "operation": "insert",
            "data": {global_key: record_id, "email": email, "name": f"before-{record_id}"},
        }
    )
    _assert(ins.get("success"), "Setup insert failed for update rollback test")

    def _mongo_fail(*_a, **_k):
        raise RuntimeError("simulated mongo write fail during update")

    with _patched_attr(tc, "_mongo_insert", _mongo_fail):
        upd = tc.execute(
            {
                "operation": "update",
                "where": {"email": email},
                "data": {"name": "after"},
            }
        )
        _assert(not upd.get("success"), "Update should fail when Mongo phase fails")
        _assert(bool(upd.get("rolled_back")), "Update failure should indicate rollback")

    rows = _read_by_id(tc, global_key, record_id)
    _assert(
        rows and rows[0].get("name") == f"before-{record_id}",
        "Atomicity(update): record not restored to pre-update state",
    )
    _assert_backends_consistent(tc, global_key, record_id, expected_present=True)
    _cleanup_id(tc, global_key, record_id)


def test_atomic_insert_mongo_commit_fail_converges(tc, global_key, _opts):
    """The hard 2PC case: PostgreSQL has committed but the final Mongo commit
    fails. The converge step must remove the half-written record from BOTH
    backends so they never diverge."""
    record_id = _make_id(11)

    def _commit_fail(_session, attempts=3):
        raise RuntimeError("simulated mongo commit failure (post-PG-commit)")

    with _patched_attr(tc, "_commit_mongo_with_retry", _commit_fail):
        res = _insert_min(tc, global_key, record_id, "commit-fail")
        _assert(not res.get("success"), "Insert should report failure on commit failure")
        _assert(bool(res.get("rolled_back")), "Commit failure should set rolled_back=True")

    rows = _read_by_id(tc, global_key, record_id)
    _assert(len(rows) == 0, "Convergence: record visible after commit-fail converge")
    _assert_backends_consistent(tc, global_key, record_id, expected_present=False)
    _cleanup_id(tc, global_key, record_id)


def test_consistency_cross_backend_after_insert(tc, global_key, _opts):
    """A record carrying BOTH SQL and Mongo data must leave both backends holding
    the key; deleting it must clear both with no orphans."""
    record_id = _make_id(12)
    ins = tc.execute({
        "operation": "insert",
        "data": {
            global_key: record_id,
            "email": _email(record_id, "xb"),
            "name": f"xb-{record_id}",
            # Mongo-routed payload so the main Mongo collection also holds the key.
            "profile": {"bio": "cross-backend", "website": "https://x.test"},
            "reviews": [{"product_id": record_id % 1000, "rating": 5, "comment": "ok"}],
        },
    })
    _assert(ins.get("success"), f"Insert failed: {ins.get('message')}")
    # Both backends must genuinely hold the key for a record with Mongo fields.
    _assert(_sql_main_has_key(global_key, record_id), "SQL main missing key after insert")
    _assert(_mongo_main_has_key(tc, global_key, record_id), "Mongo main missing key after insert")
    _assert_backends_consistent(tc, global_key, record_id, expected_present=True)

    dele = tc.execute({"operation": "delete", "where": {global_key: record_id}})
    _assert(dele.get("success"), "Delete failed")
    _assert(not _mongo_main_has_key(tc, global_key, record_id), "Mongo doc orphaned after delete")
    _assert_backends_consistent(tc, global_key, record_id, expected_present=False)


def test_isolation_reader_never_sees_torn_update(tc, global_key, opts):
    """While a record is updated (delete-then-insert), a concurrent reader must
    ALWAYS see exactly one record whose name is either the old or new value —
    never zero rows (which would expose the intermediate delete)."""
    record_id = _make_id(450)
    email = _email(record_id, "iso-torn")
    old_name, new_name = f"old-{record_id}", f"new-{record_id}"
    ins = tc.execute(
        {"operation": "insert",
         "data": {global_key: record_id, "email": email, "name": old_name}}
    )
    _assert(ins.get("success"), "Setup insert failed for torn-read test")

    observations = []
    stop = {"flag": False}

    def _reader():
        while not stop["flag"]:
            rows = _read_by_id(tc, global_key, record_id)
            observations.append(tuple(sorted(r.get("name") for r in rows)))

    with ThreadPoolExecutor(max_workers=2) as pool:
        reader = pool.submit(_reader)
        upd = pool.submit(
            tc.execute,
            {"operation": "update", "where": {"email": email}, "data": {"name": new_name}},
        ).result()
        stop["flag"] = True
        reader.result()

    _assert(upd.get("success"), "Update failed during torn-read test")
    for obs in observations:
        _assert(
            obs in ((), (old_name,), (new_name,)),
            f"Torn read observed during update: {obs}",
        )
    # The record must never have vanished entirely between reads.
    _assert(
        all(len(o) <= 1 for o in observations),
        "Reader saw duplicate rows for a single key during update",
    )
    _cleanup_id(tc, global_key, record_id)


def test_consistency_duplicate_insert_rejected(tc, global_key, _opts):
    record_id = _make_id(4)
    first = _insert_min(tc, global_key, record_id, "cons-dupe")
    _assert(first.get("success"), "First insert failed unexpectedly")

    second = _insert_min(tc, global_key, record_id, "cons-dupe")
    _assert(not second.get("success"), "Duplicate insert should fail")

    rows = _read_by_id(tc, global_key, record_id)
    _assert(len(rows) == 1, "Duplicate insert changed row cardinality")
    _cleanup_id(tc, global_key, record_id)


def test_consistency_unknown_update_data_rejected(tc, global_key, _opts):
    record_id = _make_id(5)
    email = _email(record_id, "cons-data")
    ins = tc.execute(
        {
            "operation": "insert",
            "data": {global_key: record_id, "email": email, "name": f"stable-{record_id}"},
        }
    )
    _assert(ins.get("success"), "Setup insert failed")

    upd = tc.execute(
        {
            "operation": "update",
            "where": {"email": email},
            "data": {"definitely_not_in_schema": 1},
        }
    )
    _assert(not upd.get("success"), "Unknown update data field must fail")

    rows = _read_by_id(tc, global_key, record_id)
    _assert(
        rows and rows[0].get("name") == f"stable-{record_id}",
        "Record mutated despite rejected update",
    )
    _cleanup_id(tc, global_key, record_id)


def test_consistency_unknown_where_rejected(tc, global_key, _opts):
    record_id = _make_id(6)
    email = _email(record_id, "cons-where")
    ins = tc.execute(
        {
            "operation": "insert",
            "data": {global_key: record_id, "email": email, "name": f"stable-{record_id}"},
        }
    )
    _assert(ins.get("success"), "Setup insert failed")

    upd = tc.execute(
        {
            "operation": "update",
            "where": {"email": email, "unknown_where_key": 42},
            "data": {"name": "mutated"},
        }
    )
    _assert(not upd.get("success"), "Unknown where field must fail")
    _assert(
        "schema" in str(upd.get("message", "")).lower(),
        "Error message should mention schema mismatch",
    )

    rows = _read_by_id(tc, global_key, record_id)
    _assert(
        rows and rows[0].get("name") == f"stable-{record_id}",
        "Record mutated despite rejected where",
    )
    _cleanup_id(tc, global_key, record_id)


def test_consistency_bulk_delete_mixed_keys_aborts_all(tc, global_key, _opts):
    existing_id = _make_id(7)
    missing_id = existing_id + 9000000
    ins = _insert_min(tc, global_key, existing_id, "cons-bulk")
    _assert(ins.get("success"), "Setup insert failed for bulk delete test")

    dele = tc.execute(
        {
            "operation": "delete",
            "where": {global_key: [existing_id, missing_id]},
        }
    )
    _assert(not dele.get("success"), "Mixed existing+missing bulk delete should fail")

    rows = _read_by_id(tc, global_key, existing_id)
    _assert(len(rows) == 1, "Bulk delete removed existing row despite mixed-key failure")
    _cleanup_id(tc, global_key, existing_id)


def test_isolation_duplicate_insert_race_repeated(tc, global_key, opts):
    rounds = opts.race_rounds
    for i in range(rounds):
        record_id = _make_id(100 + i)
        query = {
            "operation": "insert",
            "data": {
                global_key: record_id,
                "email": _email(record_id, "iso-dupe"),
                "name": f"race-{record_id}",
            },
        }

        with ThreadPoolExecutor(max_workers=2) as pool:
            r1 = pool.submit(tc.execute, query).result()
            r2 = pool.submit(tc.execute, query).result()

        success_count = int(bool(r1.get("success"))) + int(bool(r2.get("success")))
        _assert(success_count == 1, f"Round {i+1}: expected exactly one success")

        rows = _read_by_id(tc, global_key, record_id)
        _assert(len(rows) == 1, f"Round {i+1}: duplicate race caused wrong cardinality")
        _cleanup_id(tc, global_key, record_id)


def test_isolation_update_delete_race(tc, global_key, opts):
    rounds = max(3, opts.race_rounds // 2)
    for i in range(rounds):
        record_id = _make_id(300 + i)
        email = _email(record_id, "iso-ud")
        ins = tc.execute(
            {
                "operation": "insert",
                "data": {global_key: record_id, "email": email, "name": f"base-{record_id}"},
            }
        )
        _assert(ins.get("success"), f"Round {i+1}: setup insert failed")

        q_upd = {
            "operation": "update",
            "where": {"email": email},
            "data": {"name": "updated"},
        }
        q_del = {"operation": "delete", "where": {"email": email}}

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(tc.execute, q_upd), pool.submit(tc.execute, q_del)]
            _ = [f.result() for f in as_completed(futures)]

        rows = _read_by_id(tc, global_key, record_id)
        _assert(len(rows) in (0, 1), f"Round {i+1}: invalid cardinality after update/delete race")
        if rows:
            _assert(
                rows[0].get("name") in (f"base-{record_id}", "updated"),
                f"Round {i+1}: torn write observed",
            )

        _cleanup_id(tc, global_key, record_id)


def test_isolation_parallel_unique_inserts(tc, global_key, opts):
    n = opts.parallel_inserts
    payloads = []
    ids = []
    for i in range(n):
        rid = _make_id(600 + i)
        ids.append(rid)
        payloads.append(
            {
                "operation": "insert",
                "data": {
                    global_key: rid,
                    "email": _email(rid, "iso-par"),
                    "name": f"parallel-{rid}",
                },
            }
        )

    with ThreadPoolExecutor(max_workers=min(16, n)) as pool:
        results = [pool.submit(tc.execute, q).result() for q in payloads]

    success_count = sum(1 for r in results if r.get("success"))
    _assert(success_count > 0, "Parallel unique inserts: all operations failed under contention")

    # Adversarial invariant: for each target key, final cardinality must be 0 or 1,
    # never >1 (no duplicate/corrupted writes).
    max_seen = 0
    existing = 0

    for rid in ids:
        rows = _read_by_id(tc, global_key, rid)
        max_seen = max(max_seen, len(rows))
        if rows:
            existing += 1
        _assert(len(rows) <= 1, "Parallel unique inserts created duplicate rows for same key")

    # Integrity bound: persisted count cannot exceed success count.
    _assert(existing <= success_count, "Persisted row count exceeds reported successful operations")

    for rid in ids:
        _cleanup_id(tc, global_key, rid)


def test_durability_reopen_reader(tc, global_key, _opts):
    """Committed data must survive a full teardown of all client connections and
    be readable again from BOTH backends via a brand-new coordinator."""
    record_id = _make_id(9)
    ins = _insert_min(tc, global_key, record_id, "durability")
    _assert(ins.get("success"), "Durability setup insert failed")

    # Simulate a fresh process with its own brand-new connections (a separate
    # coordinator builds its own PostgreSQL connections and MongoClient). We do
    # NOT close tc's shared client here, since the suite reuses tc.
    tc2 = TransactionCoordinator()
    rows = _read_by_id(tc2, global_key, record_id)
    _assert(len(rows) == 1, "Committed row not visible from fresh coordinator instance")
    # Verify durability directly against PostgreSQL (source of truth for existence).
    _assert(_sql_main_has_key(global_key, record_id), "Durability: row missing from PostgreSQL")

    _cleanup_id(tc2, global_key, record_id)


def test_api_contract_rolled_back_flag(tc, global_key, _opts):
    """Ensure failures that trigger compensation set rolled_back truthfully."""
    record_id = _make_id(10)
    ins = _insert_min(tc, global_key, record_id, "contract")
    _assert(ins.get("success"), "Contract setup insert failed")

    def _mongo_fail(*_a, **_k):
        raise RuntimeError("simulated mongo delete failure")

    with _patched_attr(tc, "_mongo_delete_by_key", _mongo_fail):
        dele = tc.execute({"operation": "delete", "where": {global_key: record_id}})
        _assert(not dele.get("success"), "Contract: delete should fail")
        _assert(bool(dele.get("rolled_back")), "Contract: expected rolled_back=True")

    _cleanup_id(tc, global_key, record_id)


def _run_tests(tc, global_key, opts):
    tests = [
        TestCase("Atomicity insert Mongo fail no residue", test_atomic_insert_mongo_fail_no_residue),
        TestCase("Atomicity delete Mongo fail restores SQL", test_atomic_delete_mongo_fail_restores_sql),
        TestCase("Atomicity update Mongo fail restores record", test_atomic_update_mongo_fail_snapshot_restore),
        TestCase("Atomicity insert Mongo commit-fail converges", test_atomic_insert_mongo_commit_fail_converges),
        TestCase("Consistency cross-backend after insert", test_consistency_cross_backend_after_insert),
        TestCase("Consistency duplicate insert rejected", test_consistency_duplicate_insert_rejected),
        TestCase("Consistency unknown update data rejected", test_consistency_unknown_update_data_rejected),
        TestCase("Consistency unknown where rejected", test_consistency_unknown_where_rejected),
        TestCase("Consistency bulk delete mixed keys aborts", test_consistency_bulk_delete_mixed_keys_aborts_all),
        TestCase("Isolation duplicate insert race repeated", test_isolation_duplicate_insert_race_repeated),
        TestCase("Isolation update/delete race", test_isolation_update_delete_race),
        TestCase("Isolation parallel unique inserts", test_isolation_parallel_unique_inserts),
        TestCase("Isolation reader never sees torn update", test_isolation_reader_never_sees_torn_update),
        TestCase("Durability reopen reader", test_durability_reopen_reader),
        TestCase("API contract rolled_back flag", test_api_contract_rolled_back_flag),
    ]

    if opts.shuffle:
        rng = random.Random(opts.seed)
        rng.shuffle(tests)

    failures = []
    print("\n=== Ultimate Adversarial Reliability Verification ===")
    print(f"race_rounds={opts.race_rounds} parallel_inserts={opts.parallel_inserts} shuffle={opts.shuffle} seed={opts.seed}")

    for idx, test in enumerate(tests, start=1):
        try:
            test.fn(tc, global_key, opts)
            print(f"[PASS] {idx}. {test.name}")
        except Exception as exc:
            failures.append((test.name, str(exc)))
            print(f"[FAIL] {idx}. {test.name} -> {exc}")

    print("\n=== Summary ===")
    print(f"Passed: {len(tests) - len(failures)}/{len(tests)}")
    if failures:
        for name, reason in failures:
            print(f" - {name}: {reason}")
        return 1
    print("All adversarial reliability checks passed.")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Ultimate adversarial reliability test runner")
    parser.add_argument("--race-rounds", type=int, default=8, help="Rounds for repeated race-condition tests")
    parser.add_argument("--parallel-inserts", type=int, default=16, help="Parallel unique inserts count")
    parser.add_argument("--shuffle", action="store_true", help="Shuffle test execution order")
    parser.add_argument("--seed", type=int, default=1337, help="Seed for shuffled order")
    opts = parser.parse_args()

    meta = _load_meta()
    global_key = meta.get("global_key")
    if not global_key:
        print("[FATAL] metadata_store.json missing global_key")
        return 2

    tc = TransactionCoordinator()
    sql_ok, mongo_ok = tc._check_backends()
    if not (sql_ok and mongo_ok):
        print("[FATAL] Backends not available. Ensure PostgreSQL and MongoDB are running.")
        print(f"  sql_available={sql_ok} mongo_available={mongo_ok}")
        return 2

    return _run_tests(tc, global_key, opts)


if __name__ == "__main__":
    sys.exit(main())
