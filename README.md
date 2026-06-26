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
6. [Performance](#performance)
7. [Design trade-offs](#design-trade-offs)
8. [Rigid vs. dynamic type handling](#rigid-vs-dynamic-type-handling)
9. [Project layout](#project-layout)
10. [Configuration](#configuration)
11. [Notes](#notes)

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

## Performance

Measured on a 5,000-record dataset (~70K rows/documents). The framework adds latency over
hitting a single database directly — that's expected, because direct access does **none**
of the cross-backend routing, merging, or atomicity the framework provides. So treat
"direct DB" as a deliberately *unfair* floor; the honest story is the **trade-off**, and
how much of the overhead we removed by profiling.

| Operation | Direct DB | Framework (initial) | Framework (optimized) |
|---|---|---|---|
| Coordinated insert | ~12 ms | 219 ms (1708% over direct) | **47 ms (356%)** |
| Single-record read | ~1 ms | 126 ms | **26 ms** |
| Update | ~3 ms | 616 ms | **302 ms** |
| Ingestion / record | — | 262 ms | **50 ms** |

Profiling showed ~94% of an insert was coordination overhead — and the cause was
recreating database clients/connections on every call (a MongoClient on a replica set
also re-runs topology discovery). Reusing a **shared MongoClient** and adding a
**PostgreSQL connection pool** cut the hot paths **~5×** (insert overhead 1708% → 356%)
with **zero behavior change** — the full ACID suite still passes 15/15. The remaining
overhead is the cross-backend merge, which scales with result size (a known trade-off,
not a connection cost).

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
