# CS 432 – Databases | IIT Gandhinagar
# Assignment 2: Autonomous Normalization & CRUD Engine
# Technical Report — Hybrid Database Framework

**Team / Author:** _(fill in your name)_  
**Submission Date:** March 22, 2026  
**System Stack:** Python 3.11 · SQLite (via `sqlite3`) · MongoDB 7 (via `pymongo`) · FastAPI · SSE Streaming

---

## Table of Contents

1. [Normalization Strategy](#1-normalization-strategy)
2. [Table Creation Logic — Primary and Foreign Keys](#2-table-creation-logic)
3. [MongoDB Design Strategy](#3-mongodb-design-strategy)
4. [Metadata System](#4-metadata-system)
5. [CRUD Query Generation](#5-crud-query-generation)
6. [Performance Considerations](#6-performance-considerations)
7. [Sources of Information](#7-sources-of-information)

---

## 1. Normalization Strategy

### How the system automatically detects repeating entities and generates normalized SQL tables

Our normalization engine operates across two stages — **static schema analysis** at registration time and **dynamic frequency observation** at ingestion time — combining both to make fully automated decomposition decisions without any hardcoded table names.

---

### Stage 1 — Structural Signal Detection (Phase 1: Schema Registration)

When a user submits a JSON schema, the system reads three structural flags per field:

| Flag | Value | Signal |
|---|---|---|
| `type` | `"array"` | Repeating group detected — inherently one-to-many |
| `independent_query` | `true` | Entity is queried standalone → must be its own SQL table |
| `appendable` | `false` | Items written once → stable child table (not a growing document) |

Crucially, the system **auto-detects `one_to_many`** from the field type itself. If a field is declared as `type: "array"`, the system automatically sets `one_to_many = True` in metadata — the user never has to declare this. This removes a common source of schema misconfiguration.

**Example from our schema:**

```json
"orders": { "type": "array", "appendable": true, "independent_query": true }
"addresses": { "type": "array", "appendable": false, "independent_query": false }
```

- `orders` → `independent_query: true` → strongest rule → immediately decomposed to **SQL.orders**
- `addresses` → array + `appendable: false` → auto one-to-many → decomposed to **SQL.addresses**

Both become child tables, linked to the main `customers` table by `customer_id` as a foreign key.

---

### Stage 2 — Frequency-Based Signal Validation (Phase 3: Field Analysis)

After ingesting data, the system computes field frequency:

```
frequency = occurrence_count / total_records
```

This validates structural decisions with real data:

- `frequency < 10%` → field is too rare to commit to any backend → sent to **Buffer** (holding area)
- `10% ≤ frequency ≤ 50%` → moderate presence → routed to **MongoDB** (document)
- `frequency > 50%` → high presence → confirmed for **SQL** main table

For nested arrays and objects, frequency is used only as a guard (they still follow structural rules), not as the primary routing signal.

---

### The Full Normalization Decision Tree

```
Field received
    │
    ├── confidence = low OR frequency < 10%  →  Buffer
    │
    ├── TYPE = primitive (int/float/string/boolean)
    │       ├── freq > 50%   →  SQL main table
    │       ├── freq 10-50%  →  MongoDB document field
    │       └── freq < 10%   →  Buffer
    │
    └── TYPE = array or object
            ├── independent_query = true        →  SQL child table
            ├── TYPE = array, appendable = true
            │       ├── avg_size ≤ 5            →  MongoDB embedded array
            │       └── avg_size > 5            →  MongoDB reference collection
            ├── TYPE = array, appendable = false →  SQL child table (one_to_many auto-set)
            └── TYPE = object                   →  MongoDB embedded document
```

---

### Result for Our Schema

| Field | Detected Signal | Assigned Backend |
|---|---|---|
| `customer_id`, `email`, `name`, `age`, `signup_date` | Primitive, freq > 50% | SQL.customers |
| `phone`, `nickname` | Primitive, freq 22–32% | MongoDB document |
| `promo_code`, `beta_flag` | Primitive, freq 5–7% | Buffer |
| `orders` | Array, independent_query=true | SQL.orders |
| `addresses` | Array, appendable=false → auto one_to_many | SQL.addresses |
| `profile`, `preferences` | Object | MongoDB embedded |
| `tags` | Array, appendable=true, avg_size ≈ 2 ≤ 5 | MongoDB embedded |
| `reviews` | Array, appendable=true, avg_size ≈ 7.5 > 5 | MongoDB reference collection |
| `support_tickets` | Array, appendable=true, avg_size ≈ 7.0 > 5 | MongoDB reference collection |

---

### Post-CRUD Adaptive Reclassification

A key innovation in our system is **automatic reclassification after every CRUD operation**. After every INSERT, UPDATE, or DELETE:

1. Occurrence counts are recalculated from live data across all backends
2. Frequencies are recomputed
3. If a field's classification changes, data is **automatically migrated** to the new backend
4. Metadata is updated to reflect the new routing

This means a field that enters the system as an unknown field in the Buffer can — after enough inserts — automatically graduate to MongoDB or SQL without any user intervention. The system is self-normalizing over time.

---

## 2. Table Creation Logic

### Rules used to decide primary keys, foreign keys, and indexes

---

### Primary Key Selection — 3-Step Surrogate Logic

For every SQL table, the system follows a deterministic 3-step process:

**Step 1 — Natural Key (Unique field exists in schema)**  
If the schema declares `"unique": true` on a field in this table, that field becomes the primary key.  
Example: `customer_id` with `unique: true` → `PRIMARY KEY` on the customers table.

**Step 2 — Derived Key (Global key present in entity)**  
If no unique field exists but the global key (`customer_id`) appears as a field in the entity's array items, use it as the primary key.

**Step 3 — Surrogate Key (Auto-generated)**  
If neither condition is met, the system auto-generates a surrogate primary key named `<entity>_id` (e.g., `order_id` for the orders table) using SQLite's `AUTOINCREMENT`. The system creates this column programmatically — no user declaration needed.

This is implemented in `classification.py` → `_resolve_child_pk()`.

---

### Foreign Key Assignment

Every child SQL table receives the global key (`customer_id`) as a foreign key pointing to the main table:

```sql
CREATE TABLE orders (
    order_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    amount     REAL,
    status     TEXT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
```

The main table name is also derived dynamically from the global key using suffix stripping and pluralisation (`customer_id` → strip `_id` → `customer` → pluralise → `customers`), ensuring no table names are hardcoded anywhere in the system.

---

### Indexes

All foreign key columns on child tables are indexed to support fast join lookups:

```sql
CREATE INDEX IF NOT EXISTS idx_orders_customer_id     ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_addresses_customer_id  ON addresses(customer_id);
```

Additionally, columns declared `unique: true` in the schema receive a `UNIQUE INDEX`:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
```

All index information is recorded in `metadata_store.json` under `key_management.SQL.<table>.indexes` so the query engine knows which columns benefit from indexed lookups.

---

### Column Type Mapping

The system maps schema types to SQLite types automatically:

| Schema Type | SQLite Type |
|---|---|
| `int` | `INTEGER` |
| `float` | `REAL` |
| `string` | `TEXT` |
| `boolean` | `INTEGER` (0/1) |

---

## 3. MongoDB Design Strategy

### How the system decides between embedded documents and separate collections

---

### The Core Decision: Embed vs. Reference

MongoDB's document model offers two strategies for storing related data. Our system automates this decision using two measurable signals from the data itself.

---

### Signal 1 — `appendable` Flag (Write Pattern)

| `appendable` | Meaning | Implication |
|---|---|---|
| `false` | Entity is written once, rarely updated | Safe to embed — no rewrite penalty |
| `true` | New items are added over time | Embedding grows the document — risk of exceeding 16 MB BSON limit |

---

### Signal 2 — `avg_size` (Cardinality Measurement)

For `appendable: true` arrays, the system measures average array size from actual ingested data:

```
avg_size = total_elements / occurrence_count
```

Compared against `AVG_SIZE_THRESHOLD = 5`:

| avg_size | Decision | Rationale |
|---|---|---|
| ≤ 5 | **Mongo.embed** | Small array, fits inside parent document, fast single-document reads |
| > 5 | **Mongo.reference** | Large array, separate collection prevents document bloat, supports independent querying |

---

### Object Fields — Always Embedded

Fields declared as `type: "object"` (regardless of `appendable`) are always embedded in the main document. Objects represent tightly coupled sub-documents (like `profile` or `preferences`) that have no meaningful existence without the parent.

---

### Summary of Our MongoDB Decisions

| Field | Type | avg_size | Decision | Rationale |
|---|---|---|---|---|
| `profile` | object | — | **Embedded** | Tightly coupled sub-document |
| `preferences` | object | — | **Embedded** | User config, always read with customer |
| `tags` | array, appendable=true | 2.0 ≤ 5 | **Embedded** | Small array, no bloat risk |
| `reviews` | array, appendable=true | 7.5 > 5 | **Reference collection** | Large array, grows independently |
| `support_tickets` | array, appendable=true | 7.0 > 5 | **Reference collection** | Large, updated independently |
| `phone`, `nickname` | primitive, freq 10–50% | — | **Document field** | Present in only some records |

---

### MongoDB Collection Schema Generation

For each `Mongo.reference` entity, the system generates a validated collection schema from metadata at DB Init time:

```python
db.create_collection("reviews", validator={
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["customer_id", "product_id", "rating"],
        "properties": {
            "customer_id": {"bsonType": "int"},
            "product_id":  {"bsonType": "int"},
            "rating":      {"bsonType": "int"},
            "comment":     {"bsonType": "string"}
        }
    }
})
```

All field names, types, and `not_null` constraints are read directly from `metadata_store.json` — the schema validator is generated programmatically, not written by hand.

---

### Document Structure for Embedded Fields

For the main customer document in MongoDB:

```json
{
  "customer_id": 32772,
  "phone": "555-1234",
  "nickname": "johndoe",
  "profile": { "bio": "...", "website": "..." },
  "preferences": { "theme": "dark", "language": "en", "notifications": true },
  "tags": [{"tag": "vip"}, {"tag": "early-adopter"}]
}
```

For reference collections (`reviews`, `support_tickets`), each item is a separate document with the global key attached:

```json
{ "customer_id": 32772, "product_id": 101, "rating": 5, "comment": "Great!" }
```

---

## 4. Metadata System

### What is stored in metadata and how it drives query generation

---

### Structure of `metadata_store.json`

The metadata store is the brain of the entire system. Every routing, validation, and query generation decision is derived from it — nothing is hardcoded in application logic.

**Top-level keys:**

```json
{
  "global_key": "customer_id",
  "registered_at": "2026-03-22T09:22:34Z",
  "total_records": 101,
  "fields": { ... },
  "key_management": { ... }
}
```

---

### Per-Field Metadata (35 fields tracked for our schema)

For every field, the system stores:

| Key | Purpose | Set By |
|---|---|---|
| `type` | Data type for validation and SQL mapping | Phase 1 |
| `level` | Nesting depth (0 = top-level) | Phase 1 |
| `parent` | Parent field name for inheritance | Phase 1 |
| `children` | List of child field names | Phase 1 |
| `not_null` | Whether field is required | Phase 1 → overwritten by Phase 2 |
| `unique` | Whether field must be unique | Phase 1 → overwritten by Phase 2 |
| `appendable` | Write pattern for arrays | Phase 1 |
| `independent_query` | Standalone query flag | Phase 1 |
| `one_to_many` | Auto-set True for all arrays | Phase 1 (auto) |
| `occurrence_count` | How many records contained this field | Phase 2 |
| `total_elements` | Total array items seen (for avg_size) | Phase 2 |
| `frequency` | occurrence_count / total_records | Phase 3 |
| `avg_size` | Average array cardinality | Phase 4 |
| `storage_backend` | "SQL", "Mongo", or "Buffer" | Phase 4 |
| `storage_detail` | e.g. "SQL.orders", "Mongo.reference" | Phase 4 |

---

### Constraint Inference — Data Wins Over Schema

A key design decision: the system does not blindly trust user-declared constraints. During Phase 2 ingestion, it observes the actual data and overwrites `not_null` and `unique` based on what was actually seen:

```
not_null ← True  only if field appeared in EVERY record
unique   ← True  only if no duplicate value was seen across all records
```

This prevents false enforcement of constraints that were declared optimistically but not actually upheld by the data.

---

### Key Management Block

After Phase 5, a `key_management` block is written to metadata:

```json
"key_management": {
  "SQL": {
    "customers":  { "primary_key": "customer_id", "foreign_key": null, "columns": [...] },
    "orders":     { "primary_key": "order_id", "foreign_key": "customer_id", "surrogate": true },
    "addresses":  { "primary_key": "address_id", "foreign_key": "customer_id", "surrogate": true }
  },
  "Mongo": {
    "embed":      ["profile", "preferences", "tags", ...],
    "reference":  ["reviews", "support_tickets", ...]
  }
}
```

This block drives all CRUD operations — the query engine reads it to know which tables to JOIN, which collections to query, and how to link results.

---

### How Metadata Drives Query Generation

Every query begins with a metadata lookup, never with hardcoded names:

```python
# Read operation — field routing
for fname in requested_fields:
    backend = meta["fields"][fname]["storage_backend"]  # "SQL" / "Mongo" / "Buffer"
    detail  = meta["fields"][fname]["storage_detail"]   # "SQL.customers", "Mongo.reference", etc.
    # route to appropriate backend query builder

# Table name derivation — completely dynamic
main_table = _main_table_name(meta["global_key"])  # "customer_id" → "customers"
```

No field names, table names, or collection names appear as string literals anywhere in the query engine code.

---

## 5. CRUD Query Generation

### How the system translates a user JSON request into SQL and MongoDB queries

All four CRUD operations share the same entry pattern: the user submits a JSON query to the system, which resolves every field against metadata and generates backend-specific operations automatically.

---

### Read Operation

**Input:**
```json
{ "operation": "read", "fields": ["name", "orders", "reviews"], "where": {"customer_id": 32772} }
```

**Step 1 — Field Resolution**

The system iterates over requested fields, looks up `storage_backend` and `storage_detail` in metadata, and buckets them:

```
name    → SQL.customers  → sql_tables["customers"] = ["name"]
orders  → SQL.orders     → sql_tables["orders"]    = ["order_id", "amount", "status"]
reviews → Mongo.reference → mongo_ref_tops          = ["reviews"]
```

**Step 2 — WHERE Routing**

The `where` clause is also resolved against metadata. The global key (`customer_id`) is replicated to ALL backends as the universal join key.

**Step 3 — Backend Queries**

*SQL (with JOIN):*
```sql
SELECT c.name, o.order_id, o.amount, o.status
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE c.customer_id = 32772;
```

*MongoDB:*
```python
db["reviews"].find({"customer_id": 32772}, {"_id": 0})
```

*Buffer (MongoDB buffer collection):*
```python
db["buffer"].find({"customer_id": 32772}, {"_id": 0})
# Also checks unknown_top sub-document for any requested unknown fields
```

**Step 4 — Result Merge**

All three results are deep-merged into a single unified JSON document per `customer_id`. The user receives one clean response regardless of how many backends were queried.

---

### Insert Operation

**Input:**
```json
{ "operation": "insert", "data": { "customer_id": 77001, "name": "Diana", "email": "d@x.com",
  "orders": [{"order_id": 5001, "amount": 299.99, "status": "pending"}],
  "loyalty_tier": "gold" } }
```

**Step 1 — Flatten**  
Nested objects are converted to dot-notation (`profile.bio`), arrays kept intact.

**Step 2 — Validate**  
- Type coercion attempted for each field (e.g., `"42"` → `42` for int fields)
- `not_null` fields checked — only top-level required fields AND child fields whose parent is present in the input
- `unique` fields queried against live backends before any write begins
- Validation is atomic: if anything fails, nothing is written

**Step 3 — Route**

```
customer_id, name, email  →  sql_scalar["customers"]
orders array              →  sql_arrays["orders"]
loyalty_tier              →  unknown_top (not in metadata → buffer)
```

**Step 4 — Execute**

SQL is written in a single transaction (parent table first, then child tables). MongoDB uses `update_one` with `upsert=True` for embedded fields, and `insert_one` per item for reference collections. Buffer uses `insert_one` into the MongoDB `buffer` collection.

---

### Update Operation — Delete + Insert Strategy

Update is implemented as a full **DELETE then INSERT** cycle. This ensures consistency across all backends without requiring partial-update logic for each backend type separately.

```
1. Read existing record (full read across all backends)
2. Deep-merge old record with new data
   - Nested objects: recursive merge (unchanged sub-fields preserved)
   - Arrays: full replacement (no item-level diffing)
3. Pre-validate merged record (unique check excludes current record)
4. Delete old record
5. Insert merged record
6. Recovery: if insert fails after delete → re-insert old record
             if recovery fails → print full old record to terminal
```

---

### Delete Operation

Three delete modes, all driven by metadata:

| Mode | Trigger | Behaviour |
|---|---|---|
| Full record | `where` only | Deletes from all backends in cascade order |
| Entity delete | `entity` + `where` | Deletes only from the owning backend |
| Field-wide delete | `field` key present | Removes a column/field across all records on all backends |
| Multi-record delete | `where.global_key` is a list | Iterates and deletes each record |

**Cascade order:**
1. SQL child tables (orders, addresses) — `DELETE WHERE customer_id = ?`
2. SQL main table (customers)
3. MongoDB reference collections (reviews, support_tickets) — `delete_many`
4. MongoDB main document — `delete_one`
5. MongoDB buffer collection — `delete_many`

All SQL deletes execute in a single transaction with rollback on failure.

---

## 6. Performance Considerations

### How the design reduces query complexity and document rewriting

---

### 1. Metadata-Driven Query Planning

Because every field's backend, table, and collection name is pre-resolved in `metadata_store.json`, the query engine never performs discovery at query time. There are no `SHOW TABLES`, `describe`, or `listCollections` calls during CRUD. The routing table is consulted once per operation, in O(fields) time.

---

### 2. Selective Backend Querying

The read engine only contacts backends that actually hold requested fields. If the user reads `["name", "email"]` (both SQL), MongoDB is never contacted. If the user reads `["reviews"]` (MongoDB reference), SQLite is never queried. This avoids unnecessary network and I/O overhead.

---

### 3. Indexed Foreign Keys

All child table foreign key columns (`customer_id` on `orders`, `customer_id` on `addresses`) carry explicit indexes:

```sql
CREATE INDEX idx_orders_customer_id    ON orders(customer_id);
CREATE INDEX idx_addresses_customer_id ON addresses(customer_id);
```

Without these, every JOIN requires a full table scan. With them, lookups by `customer_id` are O(log n) regardless of table size.

---

### 4. SQL JOIN Fan-Out Prevention

When reading from multiple SQL tables, the system performs a single JOIN query rather than separate queries per table. This eliminates N+1 query patterns.

For 1:many child tables, array results are reconstructed in Python after a single JOIN, avoiding the row-multiplication problem by grouping results by the global key post-query.

---

### 5. Two-Layer Buffer Architecture

**Staging buffer** (`buffer.json`, local file) is used exclusively during ingestion. Writing 100 records to a local file in a single batch is far faster than 100 individual MongoDB insertions.

After DB Init, only buffer-relevant fields (below frequency threshold or unknown) are flushed to the **persistent buffer** (MongoDB collection). SQL and MongoDB classified fields are stripped. This means:
- Buffer collection stays small (only genuinely unclassified data)
- CRUD buffer queries are fast (small collection, indexed on global_key)

---

### 6. Embedding vs. Referencing for Write Performance

MongoDB's 16 MB BSON document size limit becomes a risk when arrays grow indefinitely. Our `avg_size` threshold (5 items) prevents this:

- `tags` (avg 2 items) → embedded → single document read, no join
- `reviews` (avg 7.5 items) → reference collection → document size stays bounded, reference collection scales independently

For fields like `preferences` (object, rarely updated) — embedding means a profile read returns all preferences in a single MongoDB document fetch, zero additional queries.

---

### 7. Atomic SQL Transactions

All SQL inserts and deletes execute within a single `BEGIN / COMMIT` transaction. If any row fails (e.g., FK constraint violation), the entire operation is rolled back. This prevents partial writes that would create inconsistent state requiring expensive repair queries.

---

### 8. Automatic Reclassification — Adaptive Optimisation

As data volume grows, field frequencies shift. Fields that initially go to Buffer (low frequency) are automatically promoted to MongoDB or SQL when they become common. This means:
- The system self-optimises over time
- Frequently-queried fields are always in the most appropriate backend for their access pattern
- No manual intervention or schema migration required from the user

---

## 7. Sources of Information

### Documentation, Research, and References

---

**Database Theory & Normalization**

1. Ramakrishnan, R., & Gehrke, J. (2003). *Database Management Systems* (3rd ed.). McGraw-Hill.  
   — Foundational reference for normal forms (1NF, 2NF, 3NF), functional dependency theory, and relational decomposition strategies.

2. Codd, E. F. (1970). *A Relational Model of Data for Large Shared Data Banks*. Communications of the ACM, 13(6), 377–387.  
   — Original paper establishing the relational model and the concept of functional dependencies.

3. Date, C. J. (2003). *An Introduction to Database Systems* (8th ed.). Addison-Wesley.  
   — Reference for foreign key constraints, referential integrity, and index design.

---

**MongoDB Design**

4. Chodorow, K. (2013). *MongoDB: The Definitive Guide* (2nd ed.). O'Reilly Media.  
   — Primary reference for embedding vs. referencing decisions, document size limits, and collection design patterns.

5. MongoDB Documentation — *Data Modeling Introduction*. https://www.mongodb.com/docs/manual/core/data-modeling-introduction/  
   — Official guidance on when to embed vs. reference, schema validation with `$jsonSchema`, and collection design.

6. MongoDB Documentation — *Schema Validation*. https://www.mongodb.com/docs/manual/core/schema-validation/  
   — Reference for generating JSON Schema validators programmatically using `create_collection`.

---

**Python & SQLite**

7. Python Software Foundation. *sqlite3 — DB-API 2.0 interface for SQLite databases*.  
   https://docs.python.org/3/library/sqlite3.html  
   — Reference for `PRAGMA foreign_keys`, `CREATE INDEX`, transaction management, and `Row` factory.

8. MongoDB. *PyMongo Documentation*. https://pymongo.readthedocs.io/  
   — Reference for `update_one` with `upsert`, `delete_many`, `aggregate`, `create_index`.

---

**Hybrid & NoSQL Systems**

9. Sadalage, P. J., & Fowler, M. (2012). *NoSQL Distilled: A Brief Guide to the Emerging World of Polyglot Persistence*. Addison-Wesley.  
   — Conceptual framework for polyglot persistence, rationale for using SQL and NoSQL together, and trade-offs between document stores and relational databases.

10. Stonebraker, M., & Cattell, R. (2011). *10 Rules for Scalable Performance in "Simple Operation" Datastores*. Communications of the ACM, 54(6), 72–80.  
    — Influenced design decisions around buffer holding area and adaptive reclassification.

---

**Streaming & API**

11. FastAPI Documentation. https://fastapi.tiangolo.com/  
    — Used for the simulation data server generating synthetic JSON records.

12. W3C. *Server-Sent Events Specification*. https://html.spec.whatwg.org/multipage/server-sent-events.html  
    — Protocol used for the real-time streaming ingestion pipeline (Phase 2).

---

*End of Report*
