# Hybrid Database Framework

An **adaptive database engine** that takes messy JSON, decides *on its own* whether each
field belongs in **PostgreSQL** (structured) or **MongoDB** (flexible) — or a temporary
**Buffer** — and exposes **one logical interface** over both. You query with plain JSON
like `{"operation":"read","fields":["name","orders"]}` and never touch SQL or Mongo
directly. Writes that span both stores run as a **single cross-backend transaction**, so
the two databases never disagree.

Built for **CS432 (Databases), IIT Gandhinagar** across four assignments: adaptive
ingestion → metadata-driven CRUD → logical dashboard + ACID validation → benchmarking
and packaging.

**Key results**
- ✅ Cross-backend atomicity verified by **15/15 adversarial ACID tests** (atomicity, consistency, isolation, durability — with injected failures).
- ⚙️ Ingests **5,000 records → ~70K rows/documents** across 6 tables and collections, autonomously split **~44% SQL / ~44% MongoDB / ~12% Buffer**.
- 🚀 Profiled and optimized the engine **~5×** (connection pooling + shared-client reuse) with no behavior change.

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
and relational — ideal for **SQL** (schemas, joins, constraints, fast structured
queries). Others are nested, sparse, or constantly changing — ideal for a **document
store** like **MongoDB** (flexible, schemaless). Traditionally a developer must decide
*up front* which database each piece of data goes into, hand-design the schema, and write
every query — manual, rigid, and brittle the moment the data changes.

**This framework removes that decision.** It observes the incoming data and *automatically*
places each field where it fits best, exposes a single logical view so you work with "a
customer" without knowing (or caring) which database each field lives in, keeps the two
stores consistent with real cross-backend transactions, and **re-places fields on the
fly** as the data evolves.

---

## Why this is hard

The interesting engineering isn't "use two databases" — it's making them behave like
**one reliable, self-organizing one**:

- **Cross-backend atomicity.** PostgreSQL and MongoDB don't share a transaction. A write
  that succeeds in one and fails in the other leaves the system *divergent* (SQL has the
  row, Mongo doesn't). Preventing that requires a coordinated commit protocol — here, a
  two-phase commit with a converge/rollback step for the failure window.
- **Deciding placement from messy data.** With no fixed schema and fields that arrive at
  different rates and types, the system must choose backends from *observed behavior*
  (frequency, structure, type stability) — and avoid "thrashing" (constantly moving fields
  back and forth) by buffering until decisions are trustworthy.
- **One record from two shapes.** A single logical record is physically split across SQL
  rows (main + child tables) and Mongo documents (embedded + reference collections). Every
  read must route, query the right backends, and **merge** the pieces back into one object.
- **Staying correct under concurrency.** Multiple writers, runtime schema migrations, and
  type drift must not corrupt data or expose half-applied state.

The rest of the README (and the benchmarks) is largely about how each of these is solved
and what it costs.

---

## Features

- **Autonomous classification** — every field routed to SQL / Mongo / Buffer by its
  *behavior* (frequency, structure, type stability), not a hand-written mapping.
- **One logical view over two databases** — reads transparently merge SQL rows and Mongo
  documents into a single record; you never see the split.
- **True cross-backend ACID transactions** — a PostgreSQL transaction + a MongoDB
  multi-document transaction committed as a unit, with rollback/convergence so the
  backends never diverge.
- **On-the-go adaptation** — fields migrate between backends at runtime as data changes
  (a rare field becoming common, or a type drifting).
- **Configurable type-conflict policy (rigid ↔ dynamic)** — safe mismatches are coerced;
  on genuine type drift the system either *migrates* the field to schemaless Mongo
  (`adaptive`, default) or *rejects* the write (`strict`). One env flag.
- **Concurrency-safe** — per-record locks + a reclassification lock, validated by
  adversarial concurrency tests.
- **Benchmarks + comparative analysis** — framework vs. direct DB access, with charts.

---

## Architecture

```
            messy JSON (stream)
                   │
        ┌──────────▼───────────┐      routing decisions persisted to
        │  Ingestion + Classify │ ───────────────────────────▶ metadata_store.json
        └──────────┬───────────┘      (survives restarts)
                   │ route each field by frequency / structure / type
        ┌──────────┼───────────────────────────┐
        ▼          ▼                            ▼
   PostgreSQL    MongoDB                      Buffer
   (frequent,   (nested objects = embed,     (rare / unknown /
    flat fields; arrays = reference;          undecided fields)
    PK/FK/idx)   collections)
        ▲          ▲                            ▲
        └────┬─────┴──────────────┬─────────────┘
             │                    │
   Transaction Coordinator   CRUD layer (read = query both + merge into one
   (2-phase commit,          record; write = split record across backends)
    per-key locks)
             │
             ▼
      Flask dashboard — shows logical entities only; never reveals tables,
                        collections, or placement decisions
```

**Pipeline:** schema registration → ingestion (to a staging buffer) → classification
(decide each field's backend) → DB init (create Postgres tables + Mongo collections, load
data) → CRUD at runtime → continuous re-classification + migration.

---

## Quick start

### 1. Prerequisites
- Python 3.11+
- Docker Desktop (provides PostgreSQL + a MongoDB replica set)

### 2. Install
```bash
pip install -r requirements.txt
```

### 3. Start the databases
```bash
docker compose up -d
```
Starts **PostgreSQL 16** on `localhost:5432` and **MongoDB 7** on `localhost:27018` as a
single-node **replica set** (`rs0`), initialised automatically. The replica set is
required for MongoDB transactions. (Port 27018 avoids clashing with a native MongoDB on
27017.)

### 4. Start the data stream (separate terminal, leave it running)
```bash
python run.py simulate
```

### 5. Build the database from the stream
```bash
python run.py --pipeline        # press Enter for the default record count
```
Ingests → classifies every field → creates Postgres tables + Mongo collections → loads data.

### 6. Create logins and open the dashboard
```bash
python run.py init-users        # admin/admin123 (developer), user/user123
python run.py dashboard         # http://localhost:5001
```

### 7. Validate and benchmark (optional)
```bash
python run.py acid              # adversarial ACID suite → expect 15/15
python run.py benchmark         # writes reports/ + charts
python -m hybriddb.analysis.comparative_analysis
```

### Shut down
```bash
docker compose down             # stop, keep data
docker compose down -v          # also wipe data for a fresh start
```

Everything is also runnable directly, e.g. `python -m hybriddb.dashboard.dashboard_app`.

---

## Benchmark results

Measured on a local machine (Docker PostgreSQL + MongoDB) over a 5,000-record dataset
(~70K rows/documents); per-operation averages from `benchmark_runner.py` +
`comparative_analysis.py`.

> **Read the overhead honestly.** The "direct DB" columns are a deliberately *unfair*
> baseline: direct access does none of the routing, cross-backend merging, or atomicity
> the framework provides. The overhead is the price of a unified, consistent, adaptive
> layer — so the number that matters is the *trade-off*, and how much of it we removed by
> profiling.

**Optimization — before vs. after (our headline result).** Profiling showed ~94% of an
insert was coordination overhead caused by recreating DB clients/connections on every
call (a MongoClient on a replica set also re-runs topology discovery). A **shared
MongoClient** + a **PostgreSQL connection pool** cut the hot paths **~5×**, with **zero
behavior change** (ACID suite still 15/15):

| Operation | Initial | Optimized | Gain |
|---|---|---|---|
| Coordinated insert | 219 ms · 1708% overhead | **47 ms · 356%** | ~4.6× |
| Single-record read | 126 ms | **26 ms** | ~4.8× |
| Update | 616 ms | **302 ms** | ~2× |
| Ingestion / record | 262 ms | **50 ms** | ~5.2× |

**Insert coordination breakdown** (where the 47 ms goes vs. direct writes):

| Component | Avg |
|---|---|
| Direct SQL insert | 3.92 ms |
| Direct MongoDB insert | 6.46 ms |
| Combined direct (SQL + Mongo) | 10.39 ms |
| **Framework coordinated insert** | **47.36 ms** |
| Coordination overhead | 36.97 ms (+356%) |

**Query latency — framework vs. direct:**

| Query | Framework | Direct SQL | Direct MongoDB |
|---|---|---|---|
| Single record (by PK) | 26.01 ms | 1.34 ms | 1.62 ms |
| All ~5,000 records | 561.96 ms | 7.76 ms | 22.87 ms |
| Update (delete + re-insert) | 302.11 ms | 3.62 ms | 4.47 ms |

**Metadata / routing cost** (negligible — and cacheable):

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
cross-backend atomicity. Metadata routing is essentially free (~0.2 ms); the real cost is
coordination on writes and the merge on multi-record reads — and connection pooling
already removed ~80% of the write overhead. The remaining merge cost scales with result
size (future work: streaming merge).

---

## Reliability (ACID validation)

Run the adversarial ACID suite:

```bash
python run.py acid          # or: python -m hybriddb.testing.reliability_test_runner
```

**Latest run: 15/15 PASS**, zero cross-backend inconsistencies, across simulated backend
failures and concurrent write rounds. Default config: **8** duplicate-insert race rounds,
**4** update/delete race rounds, **16** parallel unique inserts (seed `1337`).

| Test | Result | What it proves |
|---|:---:|---|
| Atomicity — insert, Mongo write fails | ✅ | SQL rolled back; no partial residue on either backend |
| Atomicity — delete, Mongo fails | ✅ | SQL restored from snapshot; record survives intact |
| Atomicity — update, Mongo fails | ✅ | Record restored to its exact pre-update state |
| Atomicity — Mongo *commit* fails after PG commit | ✅ | Converge step removes the half-write; backends never diverge |
| Consistency — cross-backend agreement | ✅ | After an insert, PostgreSQL and MongoDB agree the record exists |
| Consistency — duplicate insert rejected | ✅ | Second insert with same primary key rejected; row count stays 1 |
| Consistency — unknown update field rejected | ✅ | Schema-unknown data fields rejected without mutating anything |
| Consistency — unknown where-key rejected | ✅ | Schema-unknown filter keys rejected without mutation |
| Consistency — bulk delete with missing key aborts | ✅ | All-or-nothing; existing rows preserved |
| Isolation — duplicate-insert race (8 rounds) | ✅ | Concurrent duplicates yield exactly one success |
| Isolation — update vs. delete race (4 rounds) | ✅ | Never produces torn or duplicate state |
| Isolation — 16 parallel unique inserts | ✅ | At most one row per distinct key |
| Isolation — reader never sees torn update | ✅ | A concurrent reader never observes the record mid-delete |
| Durability — fresh-reader reopen | ✅ | Committed data visible from a new coordinator and directly in PostgreSQL |
| API contract — `rolled_back` flag | ✅ | Failed operations correctly report `rolled_back=True` |

Each atomicity test *injects* a failure (e.g. forces the Mongo write or its final commit
to fail) and then asserts neither backend kept a partial write — so a green result means
the system **handled** the failure correctly.

---

## Design trade-offs

**Why not just PostgreSQL?** Nested, sparse, frequently-changing data fights a rigid
relational schema; you end up with sparse columns or constant migrations.

**Why not just MongoDB?** You lose relational integrity, joins, and strict constraints
for the structured, high-frequency data that benefits from them.

**Why a hybrid layer?** Each field lands where it's strongest — automatically — behind one
interface, and the placement adapts as the data changes. The cost is the abstraction
overhead above (measured, and largely optimized away). It's worth it when data is
genuinely mixed and you'd otherwise hand-manage two databases; it's overhead you wouldn't
pay for a simple, uniform workload.

---

## Rigid vs. dynamic type handling

A user-provided schema declares each field's type. What happens when incoming data
**violates** it? That's a real trade-off, exposed as one switch —
`HYBRIDDB_TYPE_CONFLICT_POLICY` (default `adaptive`):

- **Safe / representational mismatches** (`12345 → "12345"`, `"42" → 42`) are **always
  coerced** — both modes accept these.
- **Genuinely un-coercible values** (e.g. `"forty"` into an `int` field):
  - **`adaptive` (dynamic, default)** — the field is **migrated to schemaless MongoDB**,
    preserving the value; mixed types then coexist (`age = 42` and `age = "old"` in the
    same field). Nothing is discarded.
  - **`strict` (rigid)** — the write is **rejected** with a clear message; the schema is a
    hard contract.

```bash
HYBRIDDB_TYPE_CONFLICT_POLICY=adaptive   # dynamic — migrate to Mongo (default)
HYBRIDDB_TYPE_CONFLICT_POLICY=strict     # rigid  — reject type violations
```

---

## Project layout
```
hybriddb/
├── config/      paths + DB/connection settings (one source of truth)
├── ingestion/   schema registration, ingestion, classification
├── storage/     db_init (Postgres+Mongo), buffer_store, audit_store, query_history_store
├── crud/        read / insert / update / delete operations
├── core/        sql_db (Postgres layer), clients, transaction_coordinator, reclassify_migrate, main
├── dashboard/   Flask + SocketIO logical dashboard
├── testing/     ACID / reliability test suite
├── analysis/    benchmark_runner, comparative_analysis
├── tools/       simulation_code (stream server), init_users
└── utils/       strict_json
data/            runtime data files (metadata_store.json, schema.json, …)
reports/         generated benchmark/analysis reports + charts
docker-compose.yml · requirements.txt · pyproject.toml · .env.example · run.py
```

---

## Configuration

All optional — defaults match the bundled Docker setup. Set via environment or a `.env`
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
- If MongoDB is not a replica set, the coordinator falls back to a
  snapshot-and-compensate scheme (transactions skipped, consistency still protected).
