# Hybrid Database Framework
### CS 432 – Databases | Assignment 2

An autonomous normalization and CRUD engine that ingests raw JSON records, classifies every field using schema analysis and frequency statistics, and routes data across **SQLite**, **MongoDB**, and a persistent **Buffer** — all without any hardcoded table or collection names.

---

## Table of Contents

- [Project Structure](#project-structure)
- [Architecture Overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Step-by-Step Execution](#step-by-step-execution)
  - [Step 1 — Start the Data Simulation Server](#step-1--start-the-data-simulation-server)
  - [Step 2 — Run the Main Hub](#step-2--run-the-main-hub)
  - [Step 3 — Schema Registration](#step-3--schema-registration)
  - [Step 4 — Data Ingestion](#step-4--data-ingestion)
  - [Step 5 — CRUD Operations](#step-5--crud-operations)
  - [Step 6 — Reset Everything](#step-6--reset-everything)
- [CRUD Query Examples](#crud-query-examples)
- [Configuration](#configuration)

---

## Project Structure

```
Hybrid-Database-Framework/
│
├── schema.json                   # User-defined JSON schema for incoming records
├── metadata_store.json           # Auto-generated metadata registry (DO NOT edit manually)
│
├── simulation_code.py            # FastAPI SSE server — generates synthetic customer records
│
├── phase1_schema_registration.py # Phase 1  — Parses schema.json → builds metadata_store.json
├── phase2_data_ingestion.py      # Phase 2  — Streams records, validates, writes to buffer.json
├── classification.py             # Phases 3-6 — Field analysis, storage classification, key management
├── db_init.py                    # DB Init  — Creates SQLite tables + MongoDB collections, loads buffer
│
├── buffer_store.py               # Two-layer buffer: staging (buffer.json) + persistent (MongoDB)
│
├── read_operation.py             # CRUD: Read  — routes query across SQL / Mongo / Buffer
├── insert_operation.py           # CRUD: Insert — validates, routes, and writes new records
├── update_operation.py           # CRUD: Update — delete-then-insert strategy with deep merge
├── delete_operation.py           # CRUD: Delete — full record / entity / field / multi-record
├── reclassify_migrate.py         # Post-CRUD reclassification and data migration
│
├── main.py                       # Single entry-point — interactive menu orchestrating all phases
│
├── hybrid_db.db                  # SQLite database file (auto-created, excluded from git)
├── report.md                     # Technical report answering the 7 PDF questions
└── .gitignore
```

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                         simulation_code.py                           │
│                    FastAPI SSE  →  /record endpoint                  │
└─────────────────────────────┬────────────────────────────────────────┘
                              │  streaming JSON records
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Phase 1  schema.json  ──►  metadata_store.json                      │
│  Phase 2  Validate + coerce  ──►  buffer.json  (staging)             │
│  Phase 3  Frequency analysis  (occurrence_count / total_records)     │
│  Phase 4  Storage classification  (SQL / Mongo.embed / Mongo.ref /   │
│           Buffer)                                                     │
│  Phase 5  Key management  (PK surrogate, FK injection, indexes)      │
│  Phase 6  Storage map merged into metadata_store.json                │
└──────┬───────────────────────────────┬────────────────────────────── ┘
       │                               │
       ▼                               ▼
┌─────────────┐              ┌─────────────────────┐
│  SQLite     │              │  MongoDB             │
│  hybrid_    │              │  hybrid_db           │
│  db.db      │              │                      │
│             │              │  ├─ customers (doc)  │
│  customers  │              │  ├─ reviews (ref)    │
│  orders     │              │  └─ buffer (persist) │
│  addresses  │              └─────────────────────┘
└─────────────┘
```

**Two-Buffer Architecture**

| Layer | Location | Used For |
|---|---|---|
| Staging buffer | `buffer.json` (local file) | Fast batch write during ingestion |
| Persistent buffer | `MongoDB: hybrid_db.buffer` | CRUD-time storage for low-frequency and unknown fields |

After DB Initialization, the staging buffer is flushed to MongoDB and deleted.

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.10+ | |
| MongoDB | 6+ | Must be running on `localhost:27017` |
| pip packages | see below | |

**Python packages:**

```
fastapi
uvicorn
sse-starlette
faker
pymongo
requests
```

---

## Installation

**1. Clone the repository**

```bash
git clone https://github.com/ParamTanna/DBMS_ASG_2.git
cd DBMS_ASG_2
```

**2. Install dependencies**

```bash
pip install fastapi uvicorn sse-starlette faker pymongo requests
```

**3. Ensure MongoDB is running**

```bash
# Default: mongod on localhost:27017
mongod
```

> If your MongoDB runs on a different port, update `MONGO_URI` in `main.py` (line ~74).

---

## Step-by-Step Execution

### Step 1 — Start the Data Simulation Server

The simulation server generates realistic synthetic customer records and streams them over SSE.  
Open a **separate terminal** and run:

```bash
uvicorn simulation_code:app --reload
```

You should see:

```
INFO:     Uvicorn running on http://127.0.0.1:8000
```

Keep this terminal running throughout the session.

---

### Step 2 — Run the Main Hub

In a **second terminal**, from the project folder:

```bash
python main.py
```

You will see the interactive menu:

```
================================================================
  Hybrid Database  —  Interactive Hub
================================================================
  Staging buffer : 0 records
  Mongo buffer   : 0 records
  Metadata       : NOT registered
----------------------------------------------------------------
  1. Schema Registration / Update
  2. Data Ingestion  (runs Classification + DB Init automatically)
  3. CRUD  (read / insert / update / delete)
  4. Reset Everything
  0. Exit
----------------------------------------------------------------
Choice:
```

---

### Step 3 — Schema Registration

Select **option 1**.

The system reads `schema.json`, parses every field's type, constraints, and structural flags, and writes the full metadata registry to `metadata_store.json`.

```
Choice: 1

================================================================
  SCHEMA REGISTRATION / UPDATE
================================================================
  Registered 35 fields from schema.json
  global_key : customer_id
  Saved → metadata_store.json
```

> You only need to do this once, or again if you change `schema.json`.

---

### Step 4 — Data Ingestion

Select **option 2** and enter the number of records to ingest (default 100):

```
Choice: 2

How many records to ingest? [100]: 100
```

The system will:

1. Stream records from `http://127.0.0.1:8000/record`
2. Validate each record against the schema (type coercion, `not_null`, `unique`)
3. Write all records to `buffer.json` (staging)
4. Automatically run **Phases 3–6** (classification)
5. Automatically run **DB Init** (create SQLite tables, MongoDB collections, populate from buffer)
6. Flush buffer-classified and unknown fields to the persistent MongoDB buffer
7. Delete `buffer.json`

After completion you will see a summary like:

```
  [Phase 3] Frequency analysis complete
  [Phase 4] Classification complete
  [Phase 5] Key management complete
  [Phase 6] Storage map saved to metadata_store.json

  [DB INIT] SQLite tables created: customers, orders, addresses
  [DB INIT] MongoDB collections created: customers, reviews, support_tickets, buffer
  [DB INIT] Flushed staging buffer → MongoDB buffer
```

---

### Step 5 — CRUD Operations

Select **option 3**. You will be prompted to enter a JSON query:

```
Choice: 3

Paste your JSON query (then press Enter twice):
```

See the [CRUD Query Examples](#crud-query-examples) section below for ready-to-use queries.

After each CRUD operation, the system automatically:
- Recounts field occurrence frequencies
- Re-evaluates classification thresholds
- Migrates any data whose backend has changed

---

### Step 6 — Reset Everything

Select **option 4** to wipe all data and start fresh:

- Drops all SQLite tables
- Drops all MongoDB collections (including buffer)
- Deletes `metadata_store.json`
- Clears `buffer.json` if it exists

```
Choice: 4

  This will wipe ALL data and metadata. Confirm? (yes/no): yes
  Reset complete.
```

---

## CRUD Query Examples

All queries are submitted as JSON through the `main.py` CRUD menu (option 3).

---

### Read

Read specific fields for a customer:

```json
{
  "operation": "read",
  "fields": ["customer_id", "name", "email", "orders", "reviews"],
  "where": { "customer_id": 32772 }
}
```

Read all fields:

```json
{
  "operation": "read",
  "fields": ["*"],
  "where": { "customer_id": 32772 }
}
```

---

### Insert

Full record:

```json
{
  "operation": "insert",
  "data": {
    "customer_id": 77001,
    "name": "Diana Prince",
    "email": "diana@example.com",
    "age": 30,
    "orders": [
      { "order_id": 5001, "amount": 299.99, "status": "pending" }
    ],
    "profile": { "bio": "Hero by day.", "website": "diana.io" },
    "reviews": [
      { "product_id": 101, "rating": 5, "comment": "Excellent!" }
    ]
  }
}
```

Minimal record (only required fields):

```json
{
  "operation": "insert",
  "data": {
    "customer_id": 77002,
    "email": "minimal@example.com"
  }
}
```

Record with an unknown field (goes to MongoDB buffer):

```json
{
  "operation": "insert",
  "data": {
    "customer_id": 77003,
    "email": "new@example.com",
    "name": "Test User",
    "loyalty_tier": "gold"
  }
}
```

---

### Update

Update scalar fields:

```json
{
  "operation": "update",
  "where": { "customer_id": 77001 },
  "data": {
    "name": "Diana Updated",
    "profile": { "bio": "Updated bio." }
  }
}
```

Update a specific order (entity-scoped):

```json
{
  "operation": "update",
  "entity": "orders",
  "where": { "customer_id": 77001, "order_id": 5001 },
  "data": { "amount": 349.99, "status": "shipped" }
}
```

---

### Delete

Delete a single full record:

```json
{
  "operation": "delete",
  "where": { "customer_id": 77001 }
}
```

Delete multiple full records:

```json
{
  "operation": "delete",
  "where": { "customer_id": [77001, 77002, 77003] }
}
```

Delete a specific entity only:

```json
{
  "operation": "delete",
  "entity": "orders",
  "where": { "customer_id": 77001, "order_id": 5001 }
}
```

Delete a field across all records:

```json
{
  "operation": "delete",
  "field": "nickname",
  "where": {}
}
```

---

## Configuration

All configurable constants are in `main.py` at the top of the file:

| Constant | Default | Description |
|---|---|---|
| `SCHEMA_FILE` | `schema.json` | Input schema path |
| `METADATA_FILE` | `metadata_store.json` | Metadata output path |
| `SQLITE_FILE` | `hybrid_db.db` | SQLite database file |
| `MONGO_URI` | `mongodb://localhost:27017` | MongoDB connection string |
| `MONGO_DB_NAME` | `hybrid_db` | MongoDB database name |
| `STREAM_BASE` | `http://127.0.0.1:8000/record` | Simulation server endpoint |
| `DEFAULT_COUNT` | `100` | Default ingestion batch size |

The classification thresholds are in `classification.py`:

| Constant | Default | Description |
|---|---|---|
| `FREQ_SQL_THRESHOLD` | `0.5` | Fields above this frequency go to SQL |
| `FREQ_MONGO_THRESHOLD` | `0.1` | Fields below this go to Buffer |
| `AVG_SIZE_THRESHOLD` | `5` | Arrays above this avg size become Mongo reference collections |
