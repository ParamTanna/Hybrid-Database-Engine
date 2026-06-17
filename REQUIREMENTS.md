# Requirements → Implementation Map (CS432 Track 2)

Every requirement from the four assignment PDFs, mapped to where it is
implemented. Status legend: ✅ done · ⚙️ done + hardened beyond the spec.

---

## Assignment 1 — Adaptive Ingestion & Hybrid Backend Placement

| # | Requirement | Status | Where |
|---|-------------|--------|-------|
| 1 | Consume live JSON stream | ✅ | `tools/simulation_code.py` (server), `ingestion/phase2_data_ingestion.py` (consumer) |
| 2 | Resolve key ambiguity / naming (ip vs IP, user_name vs userName) | ✅ | `ingestion/phase1_schema_registration.py`, `ingestion/classification.py` normalization |
| 3 | Handle type drifting (int→string mid-stream) | ✅ | `phase2_data_ingestion.py` validation + `crud/insert_operation.py:_coerce` |
| 4 | Handle nested dicts/arrays | ✅ | `insert_operation.flatten`, classification CASE 2 (nested) |
| 5 | Preserve `username`/global key in both backends | ✅ | global key written to SQL main + Mongo; `transaction_coordinator` join key |
| 6 | Bi-temporal timestamps (client `t_stamp` + server `sys_ingested_at`) | ✅ | `storage/audit_store.py` (created/updated), record `received_at` |
| 7 | Frequency / type-stability / structural analysis | ✅ | `classification.phase3_field_analysis` |
| 8 | Heuristic SQL vs Mongo placement with thresholds | ✅ | `classification.phase4_classify` (FREQ_RARE 10%, FREQ_SQL 50%, AVG_SIZE 5) |
| 9 | No hardcoded field mappings (dynamic discovery) | ✅ | all routing derives from `metadata_store.json` |
| 10 | Persist classification across restarts | ✅ | `data/metadata_store.json` |
| 11 | UNIQUE detection vs merely frequent | ✅ | `phase2` constraint tracking → `db_init` UNIQUE indexes |
| 12 | Differentiate IP-string vs float | ✅ | type inference in `phase2` / `classification` |

## Assignment 2 — Autonomous Normalization & CRUD Engine

| # | Requirement | Status | Where |
|---|-------------|--------|-------|
| 1 | Schema registration | ✅ | `ingestion/phase1_schema_registration.py` |
| 2 | Metadata interpretation (fields, nesting, types) | ✅ | `classification.py` phases 3–6 → metadata manager |
| 3 | Data classification SQL / Mongo / Buffer | ✅ | `classification.phase4_classify` |
| 4 | SQL normalization: repeating groups → tables | ✅ | `classification` (child tables for arrays), `storage/db_init.setup_postgres` |
| 5 | Primary keys, foreign keys, indexes | ✅ | `db_init` DDL (`IDENTITY` PKs, FK clauses, unique/plain indexes) |
| 6 | Mongo embed vs reference decision | ✅ | `classification` CASE 2 (avg array size, appendable, independent_query) |
| 7 | Create collections + sub-collections | ✅ | `db_init.setup_mongo` (main + reference collections) |
| 8 | Metadata-driven query generation | ✅ | `crud/read_operation.py` routes fields → SQL/Mongo/buffer |
| 9 | READ: translate, join/lookup, merge | ✅ | `read_operation.execute_read` + `merge_results` |
| 10 | INSERT: split record, write each backend, keep join keys | ✅ | `crud/insert_operation.py`, `transaction_coordinator._stage_sql_insert`/`_mongo_insert` |
| 11 | DELETE: full record (cascade) + specific entity | ✅ | `crud/delete_operation.py` (Cases A–D), `_sql_delete_by_key`/`_mongo_delete_by_key` |
| 12 | UPDATE (delete + insert strategy) | ✅ | `crud/update_operation.py`, coordinator delete-then-insert (one atomic unit) |
| 13 | Metadata consistency maintained | ✅ | `core/reclassify_migrate.check_and_migrate` (under reclassify lock) |

## Assignment 3 — Logical Dashboard & Transactional Validation

| # | Requirement | Status | Where |
|---|-------------|--------|-------|
| 1 | Dashboard shows data from both DBs as logical entities | ✅ | `dashboard/dashboard_app.py` |
| 2 | View active sessions | ✅ | dashboard `/sessions`, `storage/query_history_store.py` |
| 3 | List logical entities + instances + field values | ✅ | dashboard `/entities`, `/records`, field inspector |
| 4 | Submit logical queries; show input/result/status | ✅ | dashboard `/query` + query history |
| 5 | Never expose backend details (tables/collections/indexes) | ✅ | dashboard renders logical schema only; dev-only routes gated |
| 6 | Transaction coordination layer (all-or-nothing) | ⚙️ | `core/transaction_coordinator.py` — true 2PC over PG + Mongo txns |
| 7 | Detect failures + roll back partial updates | ⚙️ | `_execute_atomic` (pre-commit rollback + post-commit converge) |
| 8 | ACID validation experiments — Atomicity | ⚙️ | `testing/acid_test_runner.py` tests 1–4 (fault injection on both backends, no residue, converge) |
| 9 | …Consistency | ⚙️ | tests 5–9 (cross-backend agreement, duplicate/unknown rejection, bulk-abort) |
| 10 | …Isolation | ⚙️ | tests 10–13 (race rounds, parallel inserts, **torn-read** witness) |
| 11 | …Durability | ⚙️ | test 14 (fresh coordinator + direct PostgreSQL verification) |
| 12 | Failure & concurrency handling (robustness) | ⚙️ | per-key locks, reclassify lock, READ COMMITTED + retry |

## Assignment 4 — Dashboard Enhancement, Performance & Packaging

| # | Requirement | Status | Where |
|---|-------------|--------|-------|
| 1 | Dashboard: sessions / entities / instances / fields / results / **history** | ✅ | `dashboard/dashboard_app.py` (query history view) |
| 2 | Hide backend specifics | ✅ | logical-only rendering; `/routing-map` & `/reliability-test` developer-gated |
| 3 | Perf: ingestion latency, query response, metadata overhead, coordination overhead | ✅ | `analysis/benchmark_runner.py` |
| 4 | Metrics: avg latency, throughput, data distribution | ✅ | `benchmark_runner` → `reports/benchmark_report.md` + charts |
| 5 | Comparative: framework vs direct SQL/Mongo | ✅ | `analysis/comparative_analysis.py` |
| 6 | Visualizations (bar/line charts, tables) | ✅ | `reports/charts/*.png` |
| 7 | Packaging: repo, setup, configure backends, run ingestion/query/dashboard | ✅ | `docker-compose.yml`, `pyproject.toml`, `requirements.txt`, `.env.example`, `README.md`, `run.py` |

---

## Gaps that were closed in this pass (beyond the original submission)

1. **Real cross-backend atomicity.** Was sequential-with-rollback (update/delete
   committed PostgreSQL before touching Mongo → divergence on failure). Now a
   genuine 2PC: PG txn + Mongo multi-document txn, PG commits first, Mongo last,
   with converge-on-commit-failure. (`core/transaction_coordinator._execute_atomic`)
2. **Concurrency / multithreading safety.** Was none. Now striped per-global-key
   locks + a process-wide reclassification lock; ACID isolation tests run real
   concurrent threads including a torn-read witness.
3. **Rigorous ACID tests.** Atomicity now injects faults on the actual write
   path and asserts *no residue on either backend*; consistency adds direct
   cross-backend agreement checks; durability verifies against a fresh
   coordinator and PostgreSQL directly; a dedicated test forces a post-commit
   Mongo failure and asserts convergence.
4. **Correctness bugs fixed.** Falsy-value drop (`x = a or b` → explicit `is not
   None`) in read/update/migrate; unauthenticated `/reliability-test` &
   `/routing-map` now gated; pagination clamped; silent `except: pass` around
   inserts replaced with surfaced errors; query-history id collision; MongoClient
   leaks closed in `finally`; `datetime.utcnow()` → tz-aware.
5. **PostgreSQL instead of SQLite**, with all dialect specifics centralized in
   `core/sql_db.py` (placeholders, upsert/`ON CONFLICT`, `IDENTITY` keys,
   `information_schema` introspection, typed error handling).
6. **Reorganized** 22 flat files into a role-based `hybriddb` package with a
   single config module (`config/paths.py`) and one-command Docker setup.
7. **Configurable type-conflict policy (new).** Type mismatches are handled in
   two tiers. **Safe/representational mismatches** (`12345 → "12345"`,
   `"42" → 42`) are **always coerced** — both in the engine and the dashboard
   (this fixed a UI bug where a string field wrongly rejected a number).
   **Genuinely un-coercible values** follow `paths.TYPE_CONFLICT_POLICY`
   (env `HYBRIDDB_TYPE_CONFLICT_POLICY`, default `adaptive`):
   - `adaptive` → **widen the field to a schemaless Mongo embed**, moving
     existing SQL values across, dropping the strict column, and storing the
     value as-is. Mixed types then coexist (`age = 42` and `age = "old"` in the
     same field); a widened field is pinned to Mongo so reclassification never
     drags it back to a strict column.
     (`reclassify_migrate.widen_field_to_mongo`; the validator and read-output
     coercion honour the `type_widened` flag; classification forces such fields
     to Mongo; the global key is never widened.)
   - `strict` → reject the write with a clear message (schema as a hard
     contract).
   Both live in `transaction_coordinator._apply_type_conflict_policy`. The
   dashboard delegates type handling to the engine (no contradictory UI-side
   strict check), so the UI and API behave identically.
8. **Fixed a latent persistence bug:** `reclassify_migrate` referenced an
   undefined `METADATA_FILE`, so every CRUD-time reclassification save silently
   failed (swallowed by the coordinator). On-the-go migration now actually
   persists.
