# Hybrid Database Framework

An adaptive database engine that takes messy JSON, decides on its own whether each field
belongs in **PostgreSQL** (structured) or **MongoDB** (flexible), or a temporary
**Buffer**, and exposes one logical interface over both. You query with plain JSON like
`{"operation":"read","fields":["name","orders"]}` and never touch SQL or Mongo directly.
Writes that span both stores run as a single cross-backend transaction, so the two
databases never disagree.

Built for CS432 (Databases) at IIT Gandhinagar across four assignments: adaptive
ingestion, metadata-driven CRUD, a logical dashboard with ACID validation, and
benchmarking with packaging.

**Key results**
- Cross-backend atomicity verified by 15/15 adversarial ACID tests (atomicity, consistency, isolation, durability, with injected failures).
- Ingests 5,000 records into ~70K rows and documents across 6 tables and collections, autonomously split roughly 44% SQL, 44% MongoDB, 12% Buffer.
- Profiled and optimized the engine ~5x (connection pooling plus shared-client reuse) with no behavior change.

---

## Table of contents
1. [The problem we're solving](#the-problem-were-solving)
2. [Why this is hard](#why-this-is-hard)
3. [Features](#features)
4. [Architecture](#architecture)
5. [Quick start](#quick-start)
6. [Benchmark results](#benchmark-results)
7. [Reliability (ACID validation)](#reliability-acid-validation)
8. [Design trade-offs](#design-trade-offs)
9. [Rigid vs. dynamic type handling](#rigid-vs-dynamic-type-handling)
10. [Project layout](#project-layout)
11. [Configuration](#configuration)
12. [Notes](#notes)

---

## The problem we're solving

Real-world data is a mix of the structured and the unstructured. Some fields are uniform
and relational, which suits **SQL** (schemas, joins, constraints, fast structured
queries). Others are nested, sparse, or constantly changing, which suits a **document
store** like **MongoDB** (flexible, schemaless). Traditionally a developer has to decide
up front which database each piece of data goes into, hand-design the schema, and write
every query. That is manual, rigid, and breaks the moment the data changes.

This framework removes that decision. It observes the incoming data and automatically
places each field where it fits best, exposes a single logical view so you work with "a
customer" without knowing (or caring) which database each field lives in, keeps the two
stores consistent with real cross-backend transactions, and re-places fields on the fly
as the data evolves.

---

## Why this is hard

The interesting part is not "use two databases", it is making them behave like one
reliable, self-organizing database.

- **Cross-backend atomicity.** PostgreSQL and MongoDB do not share a transaction. A write
  that succeeds in one and fails in the other leaves the system divergent (SQL has the
  row, Mongo does not). Preventing that needs a coordinated commit protocol: here, a
  two-phase commit with a converge/rollback step for the failure window.
- **Deciding placement from messy data.** With no fixed schema and fields that arrive at
  different rates and types, the system must choose backends from observed behavior
  (frequency, structure, type stability), and avoid "thrashing" (constantly moving fields
  back and forth) by buffering until decisions are trustworthy.
- **One record from two shapes.** A single logical record is physically split across SQL
  rows (main and child tables) and Mongo documents (embedded and reference collections).
  Every read has to route, query the right backends, and merge the pieces back into one
  object.
- **Staying correct under concurrency.** Multiple writers, runtime schema migrations, and
  type drift must not corrupt data or expose half-applied state.

The rest of this document, and the benchmarks, is largely about how each of these is
solved and what it costs.

---

## Features

- **Autonomous classification.** Every field is routed to SQL, Mongo, or Buffer by its
  behavior (frequency, structure, type stability), not a hand-written mapping.
- **One logical view over two databases.** Reads transparently merge SQL rows and Mongo
  documents into a single record; you never see the split.
- **True cross-backend ACID transactions.** A PostgreSQL transaction and a MongoDB
  multi-document transaction commit as a unit, with rollback/convergence so the backends
  never diverge.
- **On-the-go adaptation.** Fields migrate between backends at runtime as data changes (a
  rare field becoming common, or a type drifting).
- **Configurable type-conflict policy (rigid or dynamic).** Safe mismatches are coerced;
  on genuine type drift the system either migrates the field to schemaless Mongo
  (`adaptive`, the default) or rejects the write (`strict`). One env flag controls it.
- **Concurrency-safe.** Per-record locks and a reclassification lock, validated by
  adversarial concurrency tests.
- **Benchmarks and comparative analysis.** Framework vs. direct DB access, with charts.

---

## Architecture

```
            messy JSON (stream)
                   |
        +----------v-----------+      routing decisions persisted to
        |  Ingestion + Classify |  ........................>  metadata_store.json
        +----------+-----------+      (survives restarts)
                   | route each field by frequency / structure / type
        +----------+---------------------------+
        v          v                           v
   PostgreSQL    MongoDB                      Buffer
   (frequent,   (nested objects = embed,     (rare / unknown /
    flat fields; arrays = reference;          undecided fields)
    PK/FK/idx)   collections)
        ^          ^                           ^
        +----+-----+-------------+-------------+
             |                   |
   Transaction Coordinator   CRUD layer (read = query both + merge into one
   (2-phase commit,          record; write = split record across backends)
    per-key locks)
             |
             v
      Flask dashboard (shows logical entities only; never reveals tables,
                       collections, or placement decisions)
```

**Pipeline:** schema registration, then ingestion into a staging buffer, then
classification (decide each field's backend), then DB init (create Postgres tables and
Mongo collections and load the data), then CRUD at runtime, with continuous
re-classification and migration as more data arrives.

---

## Quick start

### 1. Prerequisites
- Python 3.11+
- Docker Desktop (provides PostgreSQL and a MongoDB replica set)

### 2. Install
```bash
pip install -r requirements.txt
```

### 3. Start the databases
```bash
docker compose up -d
```
Starts PostgreSQL 16 on `localhost:5432` and MongoDB 7 on `localhost:27018` as a
single-node replica set (`rs0`), initialised automatically. The replica set is required
for MongoDB transactions. Port 27018 avoids clashing with a native MongoDB on 27017.

### 4. Start the data stream (separate terminal, leave it running)
```bash
python run.py simulate
```

### 5. Build the database from the stream
```bash
python run.py --pipeline        # press Enter for the default record count
```
Ingests records, classifies every field, creates the Postgres tables and Mongo
collections, then loads the data.

### 6. Create logins and open the dashboard
```bash
python run.py init-users        # admin/admin123 (developer), user/user123
python run.py dashboard         # http://localhost:5001
```

### 7. Validate and benchmark (optional)
```bash
python run.py acid              # adversarial ACID suite, expect 15/15
python run.py benchmark         # writes reports/ and charts
python -m hybriddb.analysis.comparative_analysis
```

### Shut down
```bash
docker compose down             # stop, keep data
docker compose down -v          # also wipe data for a fresh start
```

Everything is also runnable directly, for example `python -m hybriddb.dashboard.dashboard_app`.

---

## Benchmark results

Measured on a local machine (Docker PostgreSQL and MongoDB) over a 5,000-record dataset
(~70K rows and documents). Per-operation averages from `benchmark_runner.py` and
`comparative_analysis.py`.

A note on reading the overhead: the "direct DB" columns are a deliberately unfair
baseline, because direct access does none of the routing, cross-backend merging, or
atomicity the framework provides. The overhead is the price of a unified, consistent,
adaptive layer, so the number that matters is the trade-off and how much of it profiling
removed.

**Optimization, before vs. after.** Profiling showed about 94% of an insert was
coordination overhead caused by recreating DB clients and connections on every call (a
MongoClient on a replica set also re-runs topology discovery). A shared MongoClient and a
PostgreSQL connection pool cut the hot paths roughly 5x, with no behavior change (the ACID
suite still passes 15/15):

| Operation | Initial | Optimized | Gain |
|---|---|---|---|
| Coordinated insert | 219 ms (1708% overhead) | 47 ms (356%) | ~4.6x |
| Single-record read | 126 ms | 26 ms | ~4.8x |
| Update | 616 ms | 302 ms | ~2x |
| Ingestion per record | 262 ms | 50 ms | ~5.2x |

**Insert coordination breakdown** (where the 47 ms goes, vs. direct writes):

| Component | Avg |
|---|---|
| Direct SQL insert | 3.92 ms |
| Direct MongoDB insert | 6.46 ms |
| Combined direct (SQL + Mongo) | 10.39 ms |
| Framework coordinated insert | 47.36 ms |
| Coordination overhead | 36.97 ms (+356%) |

**Query latency, framework vs. direct:**

| Query | Framework | Direct SQL | Direct MongoDB |
|---|---|---|---|
| Single record (by PK) | 26.01 ms | 1.34 ms | 1.62 ms |
| All ~5,000 records | 561.96 ms | 7.76 ms | 22.87 ms |
| Update (delete + re-insert) | 302.11 ms | 3.62 ms | 4.47 ms |

**Metadata and routing cost** (negligible, and cacheable):

| Operation | Avg |
|---|---|
| Load `metadata_store.json` | 0.207 ms |
| Single field routing lookup | 0.00015 ms |
| Wildcard field expansion | 0.0021 ms |

**Autonomous field-routing distribution** (16 top-level fields, decided by the engine):

| Backend | Fields | Share |
|---|---|---|
| PostgreSQL | 7 | 43.8% |
| MongoDB (embedded) | 5 | 31.2% |
| MongoDB (reference) | 2 | 12.5% |
| Buffer | 2 | 12.5% |

**Charts** (written to `reports/charts/` after a run): `latency_comparison.png`,
`coordination_overhead.png`, `overhead_breakdown.png`, `data_distribution.png`,
`comparative_read_latency.png`, `comparative_update_latency.png`.

**Takeaway.** The framework trades raw speed for a unified API, schema-driven routing, and
cross-backend atomicity. Metadata routing is essentially free (about 0.2 ms); the real
cost is coordination on writes and the merge on multi-record reads, and connection pooling
already removed roughly 80% of the write overhead. The remaining merge cost scales with
result size (future work: a streaming merge).

---

## Reliability (ACID validation)

Run the adversarial ACID suite:

```bash
python run.py acid          # or: python -m hybriddb.testing.reliability_test_runner
```

Latest run: 15/15 pass, zero cross-backend inconsistencies, across simulated backend
failures and concurrent write rounds. Default config: 8 duplicate-insert race rounds, 4
update/delete race rounds, 16 parallel unique inserts (seed `1337`).

| Test | Result | What it proves |
|---|:---:|---|
| Atomicity, insert with Mongo write failure | Pass | SQL rolled back; no partial residue on either backend |
| Atomicity, delete with Mongo failure | Pass | SQL restored from snapshot; record survives intact |
| Atomicity, update with Mongo failure | Pass | Record restored to its exact pre-update state |
| Atomicity, Mongo commit fails after PG commit | Pass | Converge step removes the half-write; backends never diverge |
| Consistency, cross-backend agreement | Pass | After an insert, PostgreSQL and MongoDB agree the record exists |
| Consistency, duplicate insert rejected | Pass | Second insert with same primary key is rejected; row count stays 1 |
| Consistency, unknown update field rejected | Pass | Schema-unknown data fields are rejected without mutating anything |
| Consistency, unknown where-key rejected | Pass | Schema-unknown filter keys are rejected without mutation |
| Consistency, bulk delete with missing key aborts | Pass | All-or-nothing; existing rows preserved |
| Isolation, duplicate-insert race (8 rounds) | Pass | Concurrent duplicates yield exactly one success |
| Isolation, update vs. delete race (4 rounds) | Pass | Never produces torn or duplicate state |
| Isolation, 16 parallel unique inserts | Pass | At most one row per distinct key |
| Isolation, reader never sees a torn update | Pass | A concurrent reader never observes the record mid-delete |
| Durability, fresh-reader reopen | Pass | Committed data visible from a new coordinator and directly in PostgreSQL |
| API contract, rolled_back flag | Pass | Failed operations correctly report `rolled_back=True` |

Each atomicity test injects a failure (for example, it forces the Mongo write or its final
commit to fail) and then asserts that neither backend kept a partial write. So a passing
result means the system handled the failure correctly.

---

## Design trade-offs

**Why not just PostgreSQL?** Nested, sparse, frequently-changing data fights a rigid
relational schema; you end up with sparse columns or constant migrations.

**Why not just MongoDB?** You lose relational integrity, joins, and strict constraints for
the structured, high-frequency data that benefits from them.

**Why a hybrid layer?** Each field lands where it is strongest, automatically, behind one
interface, and the placement adapts as the data changes. The cost is the abstraction
overhead shown above (measured, and largely optimized away). It is worth it when data is
genuinely mixed and you would otherwise hand-manage two databases; it is overhead you
would not pay for a simple, uniform workload.

---

## Rigid vs. dynamic type handling

A user-provided schema declares each field's type. What happens when incoming data
violates it? That is a real trade-off, exposed as one switch,
`HYBRIDDB_TYPE_CONFLICT_POLICY` (default `adaptive`):

- Safe or representational mismatches (`12345` to `"12345"`, `"42"` to `42`) are always
  coerced; both modes accept these.
- Genuinely un-coercible values (for example `"forty"` into an `int` field):
  - `adaptive` (dynamic, the default): the field is migrated to schemaless MongoDB,
    preserving the value, so mixed types then coexist (`age = 42` and `age = "old"` in the
    same field). Nothing is discarded.
  - `strict` (rigid): the write is rejected with a clear message; the schema is a hard
    contract.

```bash
HYBRIDDB_TYPE_CONFLICT_POLICY=adaptive   # dynamic: migrate to Mongo (default)
HYBRIDDB_TYPE_CONFLICT_POLICY=strict     # rigid: reject type violations
```

---

## Project layout
```
hybriddb/
  config/      paths + DB/connection settings (one source of truth)
  ingestion/   schema registration, ingestion, classification
  storage/     db_init (Postgres+Mongo), buffer_store, audit_store, query_history_store
  crud/        read / insert / update / delete operations
  core/        sql_db (Postgres layer), clients, transaction_coordinator, reclassify_migrate, main
  dashboard/   Flask + SocketIO logical dashboard
  testing/     ACID / reliability test suite
  analysis/    benchmark_runner, comparative_analysis
  tools/       simulation_code (stream server), init_users
  utils/       strict_json
data/          runtime data files (metadata_store.json, schema.json, ...)
reports/       generated benchmark/analysis reports and charts
docker-compose.yml, requirements.txt, pyproject.toml, .env.example, run.py
```

---

## Configuration

All optional; defaults match the bundled Docker setup. Set via environment or a `.env`
file (see `.env.example`).

| Variable | Default | Meaning |
|----------|---------|---------|
| `PGHOST` / `PGPORT` / `PGDATABASE` / `PGUSER` / `PGPASSWORD` | localhost / 5432 / hybrid_db / postgres / postgres | PostgreSQL |
| `MONGO_URI` | `mongodb://localhost:27018/?replicaSet=rs0` | MongoDB (replica set) |
| `MONGO_DB_NAME` | `hybrid_db` | Mongo database |
| `STREAM_BASE` | `http://127.0.0.1:8000/record` | Ingestion stream |
| `HYBRIDDB_TYPE_CONFLICT_POLICY` | `adaptive` | `adaptive` (migrate to Mongo) or `strict` (reject) on un-coercible type drift |
| `HYBRIDDB_DATA_DIR` | `./data` | Runtime data-file location |

---

## Notes
- All PostgreSQL dialect specifics are centralized in `hybriddb/core/sql_db.py`.
- If MongoDB is not a replica set, the coordinator falls back to a snapshot-and-compensate
  scheme (transactions are skipped, consistency is still protected).
