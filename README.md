# Hybrid Database Framework (CS432 Track 2)

An **adaptive database engine** that takes messy JSON, decides *on its own* whether
each field belongs in **PostgreSQL** (structured) or **MongoDB** (flexible) — or a
temporary **Buffer** — and then gives you **one simple logical interface** over both.
Writes that span both databases run as a **true cross-backend transaction**, so the
two stores never disagree.

You query it with plain JSON like `{"operation":"read","fields":["name","orders"]}`
and never touch SQL or Mongo directly.

---

## The problem we're solving

Real-world data is a mix of the structured and the unstructured. Some fields are
uniform and relational — perfect for **SQL** (schemas, joins, constraints, fast
structured queries). Others are nested, sparse, or constantly changing — perfect for a
**document store** like **MongoDB** (flexible, schemaless). Traditionally a developer
must decide *up front* which database each piece of data goes into, hand-design the
schema, and write every query — which is manual, rigid, and breaks the moment the data
changes.

**This framework removes that decision.** It watches the incoming data and
*automatically* places each field where it fits best — PostgreSQL for the
structured/frequent parts, MongoDB for the nested/variable parts — and exposes a single
**logical interface** so you work with "a customer" without ever knowing (or caring)
which database each field lives in. It keeps the two stores consistent using real
cross-backend transactions, and **re-places fields on the fly** as the data evolves.
The best of both worlds — chosen and maintained for you, automatically.

---

## What it does (the four assignments)

| Part | Theme |
|------|-------|
| **A1** | Adaptive ingestion + autonomous SQL/Mongo placement |
| **A2** | Auto-normalization + metadata-driven CRUD |
| **A3** | Logical dashboard + ACID transaction coordination |
| **A4** | Dashboard enhancement + benchmarking + packaging |

**Headline features**
-  **Autonomous classification** — fields routed to SQL / Mongo / Buffer by their *behavior* (frequency, structure, type), no manual schema mapping.
-  **One logical view over two databases** — reads transparently merge SQL rows + Mongo docs into a single record.
-  **True cross-backend ACID transactions** — PostgreSQL transaction + MongoDB multi-document transaction, committed as a unit, with rollback/convergence so the backends never diverge.
-  **On-the-go adaptation** — fields migrate between backends at runtime as data changes (e.g. a rare field becoming common, or a type drifting).
-  **Configurable type-conflict policy (rigid ↔ dynamic)** — safe mismatches are always coerced; on genuine type drift the system either **migrates** the field to schemaless Mongo (`adaptive` / dynamic — the default) or **rejects** the write (`strict` / rigid). One env flag flips it.
-  **Concurrency-safe** — per-record locks + a reclassification lock; validated with adversarial ACID tests.
-  **Benchmarks + comparative analysis** — framework vs. direct DB access, with charts.

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
Starts **PostgreSQL 16** on `localhost:5432` and **MongoDB 7** on `localhost:27018`
as a single-node **replica set** (`rs0`), initialised automatically. The replica set
is required for MongoDB transactions. (Port 27018 avoids clashing with any native
MongoDB already on 27017.)

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

### 7. Validate + benchmark (optional)
```bash
python run.py acid              # 15 adversarial ACID tests → expect 15/15
python run.py benchmark         # writes reports/ + charts
python -m hybriddb.analysis.comparative_analysis
```

### Shut down
```bash
docker compose down             # keep data
docker compose down -v          # wipe data for a fresh start
```

Everything is also runnable directly, e.g. `python -m hybriddb.dashboard.dashboard_app`.

---

## Folder layout
```
hybriddb/
├── config/      paths + DB/connection settings (one source of truth)
├── ingestion/   schema registration, ingestion, classification
├── storage/     db_init (Postgres+Mongo), buffer_store, audit_store, query_history_store
├── crud/        read / insert / update / delete operations
├── core/        sql_db (Postgres layer), transaction_coordinator, reclassify_migrate, main
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

## Configuration (`.env`, all optional — defaults match Docker)

| Variable | Default | Meaning |
|----------|---------|---------|
| `PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD` | localhost/5432/hybrid_db/postgres/postgres | PostgreSQL |
| `MONGO_URI` | `mongodb://localhost:27018/?replicaSet=rs0` | MongoDB (replica set) |
| `MONGO_DB_NAME` | `hybrid_db` | Mongo database |
| `STREAM_BASE` | `http://127.0.0.1:8000/record` | Ingestion stream |
| `HYBRIDDB_TYPE_CONFLICT_POLICY` | `adaptive` | `adaptive` (widen to Mongo) or `strict` (reject) on un-coercible type drift |
| `HYBRIDDB_DATA_DIR` | `./data` | Runtime data-file location |

---

## How it works (in one paragraph)
The **classifier** scores each field by frequency, structure and type and writes its
decision to `data/metadata_store.json` (so decisions survive restarts). **db_init**
turns that metadata into Postgres tables (with keys/indexes) and Mongo collections.
The **CRUD layer** translates one JSON request into the right SQL + Mongo queries and
merges the results. The **transaction coordinator** runs cross-backend writes as a
two-phase commit and re-checks classification after each write, migrating fields whose
behavior changed. The **dashboard** shows everything as logical entities and never
exposes the underlying tables or collections.

## Rigid vs. dynamic type handling

A user-provided schema declares each field's type. What should happen when incoming
data **violates** that type? That's a genuine design trade-off, so it's a single switch —
`HYBRIDDB_TYPE_CONFLICT_POLICY` (default `adaptive`):

- **Safe / representational mismatches** (`12345 → "12345"`, `"42" → 42`) are **always
  coerced** — both modes accept these.
- **Genuinely un-coercible values** (e.g. `"forty"` into an `int` field):
  - **`adaptive` (dynamic, default)** → the field is **migrated to schemaless MongoDB**,
    preserving the value; mixed types then coexist (`age = 42` and `age = "old"` in the
    same field). Nothing is discarded.
  - **`strict` (rigid)** → the write is **rejected** with a clear message; the schema is
    enforced as a hard contract.

Flip it any time (env var or `.env`):
```bash
HYBRIDDB_TYPE_CONFLICT_POLICY=adaptive   # dynamic — migrate to Mongo (default)
HYBRIDDB_TYPE_CONFLICT_POLICY=strict     # rigid  — reject type violations
```

## Notes
- All PostgreSQL dialect specifics are centralized in `hybriddb/core/sql_db.py`.
- If MongoDB is not a replica set, the coordinator falls back to a
  snapshot-and-compensate scheme (transactions skipped, consistency still protected).
