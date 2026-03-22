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

Our normalization engine operates across two stages — **static schema analysis** at registration time and **dynamic frequency observation** at ingestion time — combining both to make fully automated decomposition decisions without any hardcoded table or field names.

---

### Stage 1 — Structural Signal Detection (Phase 1: Schema Registration)

When a user submits a JSON schema, the system reads three structural flags per field:

| Flag | Value | Signal |
|---|---|---|
| `type` | `"array"` | Repeating group detected — inherently one-to-many |
| `independent_query` | `true` | Entity is queried standalone → must be its own SQL table |
| `appendable` | `false` | Items written once → stable child table (not a growing document) |

Crucially, the system **auto-detects `one_to_many`** from the field type itself. If a field is declared as `type: "array"`, the system automatically sets `one_to_many = True` in metadata — the user never has to declare this explicitly. This removes a common source of schema misconfiguration.

Consider two hypothetical array fields in any schema:

```json
"<array_entity_A>": { "type": "array", "appendable": true,  "independent_query": true  }
"<array_entity_B>": { "type": "array", "appendable": false, "independent_query": false }
```

- `<array_entity_A>` → `independent_query: true` → strongest rule → immediately decomposed to its own **SQL child table**
- `<array_entity_B>` → array + `appendable: false` → auto one-to-many → decomposed to **SQL child table**

Both become child tables, linked to the main table by the global key as a foreign key.

---

### Stage 2 — Frequency-Based Signal Validation (Phase 3: Field Analysis)

After ingesting data, the system computes field frequency:

```
frequency = occurrence_count / total_records
```

This validates structural decisions with real data:

- `frequency < 10%` → field is too rare to commit to any backend → sent to **Buffer** (holding area)
- `10% ≤ frequency ≤ 50%` → moderate presence → routed to **MongoDB** (document field)
- `frequency > 50%` → high presence → confirmed for **SQL** main table

For nested arrays and objects, frequency is used only as a guard (they still follow structural rules), not as the primary routing signal.

---

### The Full Normalization Decision Tree

```
Field received
    │
    ├── frequency < 10%  →  Buffer
    │
    ├── TYPE = primitive (int/float/string/boolean)
    │       ├── freq > 50%   →  SQL main table
    │       ├── freq 10-50%  →  MongoDB document field
    │       └── freq < 10%   →  Buffer
    │
    └── TYPE = array or object
            ├── independent_query = true         →  SQL child table
            ├── TYPE = array, appendable = true
            │       ├── avg_size ≤ threshold     →  MongoDB embedded array
            │       └── avg_size > threshold     →  MongoDB reference collection
            ├── TYPE = array, appendable = false  →  SQL child table (one_to_many auto-set)
            └── TYPE = object                    →  MongoDB embedded document
```

---

### Classification Outcome Categories

Regardless of what schema a user provides, all fields fall into one of these five categories:

| Category | Condition | Storage |
|---|---|---|
| **SQL main table** | Primitive, freq > 50% | SQLite main table |
| **SQL child table** | Array with `independent_query=true` OR `appendable=false` | SQLite child table with FK |
| **MongoDB document** | Primitive, freq 10–50%, OR `type: "object"` | MongoDB main collection |
| **MongoDB reference** | Array, `appendable=true`, avg_size > threshold | Separate MongoDB collection |
| **Buffer** | Primitive freq < 10%, OR unknown field not in schema | MongoDB buffer collection |

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
If the schema declares `"unique": true` on any field that belongs to this table, that field becomes the primary key.

**Step 2 — Derived Key (Global key present in entity)**  
If no unique field exists but the global key appears as a field within the entity's array items, the global key is used as the primary key of that child table.

**Step 3 — Surrogate Key (Auto-generated)**  
If neither condition is met, the system auto-generates a surrogate primary key named `<entity_name>_id` using SQLite's `AUTOINCREMENT`. The system creates this column programmatically — no user declaration needed. This ensures every table is always identifiable regardless of what fields the user defined.

This logic is implemented in `classification.py` → `_resolve_child_pk()`.

---

### Foreign Key Assignment

Every child SQL table automatically receives the global key as a foreign key pointing to the main table. The main table name itself is derived dynamically from the global key via suffix stripping and pluralisation — for example, a global key named `<entity>_id` produces a main table named `<entities>`. Nothing is hardcoded.

```sql
-- Generic child table structure
CREATE TABLE <child_entity> (
    <child_entity>_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    <global_key>       INTEGER NOT NULL,
    <field_1>          <TYPE>,
    <field_2>          <TYPE>,
    FOREIGN KEY (<global_key>) REFERENCES <main_table>(<global_key>)
);
```

---

### Indexes

All foreign key columns on child tables receive an index to support fast join lookups:

```sql
CREATE INDEX IF NOT EXISTS idx_<child_table>_<global_key> ON <child_table>(<global_key>);
```

Additionally, columns declared `unique: true` in the schema receive a `UNIQUE INDEX`:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_<main_table>_<unique_field> ON <main_table>(<unique_field>);
```

All index information is recorded in `metadata_store.json` under `key_management.SQL.<table>.indexes` so the query engine knows which columns benefit from indexed lookups at query time.

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

MongoDB's document model offers two strategies for storing related data. Our system automates this decision using two measurable signals derived from the schema and actual ingested data — no manual annotation required.

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

Compared against a configurable `AVG_SIZE_THRESHOLD`:

| avg_size | Decision | Rationale |
|---|---|---|
| ≤ threshold | **Mongo.embed** | Small array, fits inside parent document, fast single-document reads |
| > threshold | **Mongo.reference** | Large array, separate collection prevents document bloat, supports independent querying |

---

### Object Fields — Always Embedded

Fields declared as `type: "object"` (regardless of `appendable`) are always embedded in the main document. Objects represent tightly coupled sub-documents that have no meaningful existence without the parent record.

---

### Classification Summary by Field Type

| Field Type | Condition | MongoDB Storage |
|---|---|---|
| Primitive scalar | freq 10–50% | Field in main document |
| `type: "object"` | any | Embedded sub-document |
| `type: "array"`, `appendable: true` | avg_size ≤ threshold | Embedded array in main document |
| `type: "array"`, `appendable: true` | avg_size > threshold | Separate reference collection |

---

### MongoDB Collection Schema Generation

For each `Mongo.reference` entity, the system auto-generates a validated collection schema from metadata at DB Init time using MongoDB's native `$jsonSchema` validator:

```python
db.create_collection("<reference_entity>", validator={
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["<global_key>", "<not_null_field_1>", "<not_null_field_2>"],
        "properties": {
            "<global_key>":      {"bsonType": "int"},
            "<not_null_field_1>": {"bsonType": "int"},
            "<not_null_field_2>": {"bsonType": "string"}
        }
    }
})
```

All field names, types, and `not_null` constraints are read directly from `metadata_store.json` — the validator is generated programmatically, not written by hand. This applies to every reference collection regardless of how many are present in the schema.

---

### Document Structures

For any entity classified as **Mongo.embed**, its data lives inside the main document:

```json
{
  "<global_key>": 12345,
  "<scalar_mongo_field>": "value",
  "<object_field>": { "<subfield_1>": "...", "<subfield_2>": "..." },
  "<small_array_field>": [{ "<item_field>": "value" }]
}
```

For any entity classified as **Mongo.reference**, each item is a separate document with the global key attached:

```json
{ "<global_key>": 12345, "<item_field_1>": "...", "<item_field_2>": "..." }
```

---

## 4. Metadata System

### What is stored in metadata and how it drives query generation

---

### Structure of `metadata_store.json`

The metadata store is the brain of the entire system. Every routing, validation, and query generation decision is derived from it — nothing is hardcoded in application logic. The same pipeline works for any schema the user provides because all names and rules are read at runtime from this file.

**Top-level keys:**

```json
{
  "global_key": "<entity_id>",
  "registered_at": "<ISO timestamp>",
  "total_records": <N>,
  "fields": { ... },
  "key_management": { ... }
}
```

---

### Per-Field Metadata

For every field in the schema, the system stores and tracks the following attributes:

| Key | Purpose | Set By |
|---|---|---|
| `type` | Data type for validation and SQL mapping | Phase 1 |
| `level` | Nesting depth (0 = top-level) | Phase 1 |
| `parent` | Parent field name for inheritance rules | Phase 1 |
| `children` | List of child field names | Phase 1 |
| `not_null` | Whether field is required | Phase 1 → overwritten by Phase 2 |
| `unique` | Whether field must be unique | Phase 1 → overwritten by Phase 2 |
| `appendable` | Write pattern for arrays | Phase 1 |
| `independent_query` | Standalone query flag | Phase 1 |
| `one_to_many` | Auto-set `True` for all array fields | Phase 1 (auto) |
| `occurrence_count` | How many records contained this field | Phase 2 |
| `total_elements` | Total array items seen (used for avg_size) | Phase 2 |
| `frequency` | `occurrence_count / total_records` | Phase 3 |
| `avg_size` | Average array cardinality | Phase 4 |
| `storage_backend` | `"SQL"`, `"Mongo"`, or `"Buffer"` | Phase 4 |
| `storage_detail` | e.g. `"SQL.<table>"`, `"Mongo.reference"` | Phase 4 |

---

### Constraint Inference — Data Wins Over Schema

A key design decision: the system does not blindly trust user-declared constraints. During Phase 2 ingestion, it observes actual data and **overwrites** `not_null` and `unique` based on what was empirically seen:

```
not_null ← True  only if the field appeared in EVERY ingested record
unique   ← True  only if no duplicate value was seen across all ingested records
```

If a user declares `not_null: true` in the schema but 30% of records arrive without that field, the system corrects the constraint to `false`. This prevents false enforcement of constraints that were declared optimistically but not actually upheld by the data.

---

### Key Management Block

After Phase 5, a `key_management` block is written to metadata capturing the full relational and document structure:

```json
"key_management": {
  "SQL": {
    "<main_table>":  { "primary_key": "<global_key>", "foreign_key": null, "columns": [...] },
    "<child_table_A>": { "primary_key": "<child_table_A>_id", "foreign_key": "<global_key>", "surrogate": true },
    "<child_table_B>": { "primary_key": "<child_table_B>_id", "foreign_key": "<global_key>", "surrogate": true }
  },
  "Mongo": {
    "embed":     ["<object_field>", "<small_array_field>", ...],
    "reference": ["<large_array_field_A>", "<large_array_field_B>", ...]
  }
}
```

This block drives all CRUD operations — the query engine reads it to know which tables to JOIN, which collections to query, and how to link results across backends.

---

### How Metadata Drives Query Generation

Every query begins with a metadata lookup, never with hardcoded names:

```python
# Read operation — field routing (works for any schema)
for fname in requested_fields:
    backend = meta["fields"][fname]["storage_backend"]  # "SQL" / "Mongo" / "Buffer"
    detail  = meta["fields"][fname]["storage_detail"]   # "SQL.<table>", "Mongo.reference", etc.
    # route to the appropriate backend query builder

# Table name derivation — completely dynamic
main_table = _main_table_name(meta["global_key"])
# e.g. "user_id" → "users", "product_id" → "products", "order_id" → "orders"
```

No field names, table names, or collection names appear as string literals anywhere in the query engine code.

---

## 5. CRUD Query Generation

### How the system translates a user JSON request into SQL and MongoDB queries

All four CRUD operations share the same entry pattern: the user submits a JSON query, and the system resolves every field against metadata to generate backend-specific operations automatically.

---

### Read Operation

**Input format:**
```json
{
  "operation": "read",
  "fields": ["<field_A>", "<child_entity>", "<reference_entity>"],
  "where": { "<global_key>": <value> }
}
```

**Step 1 — Field Resolution**

The system iterates over requested fields, looks up `storage_backend` and `storage_detail` in metadata, and groups them by backend:

```
<scalar_field>    → SQL.<main_table>   → sql_bucket["<main_table>"]
<child_entity>    → SQL.<child_table>  → sql_bucket["<child_table>"]
<ref_entity>      → Mongo.reference   → mongo_ref_bucket
<embed_field>     → Mongo.embed       → mongo_embed_bucket
<buffer_field>    → Buffer            → buffer_bucket
```

**Step 2 — WHERE Routing**

The `where` clause is resolved against metadata. The global key is replicated to all backends as the universal join key.

**Step 3 — Backend Queries**

*SQL — single JOIN spanning all requested tables:*
```sql
SELECT t1.<field_A>, t2.<child_field_1>, t2.<child_field_2>
FROM <main_table> t1
LEFT JOIN <child_table> t2 ON t1.<global_key> = t2.<global_key>
WHERE t1.<global_key> = <value>;
```

*MongoDB — projection on main document (embed) and per-reference collection query:*
```python
db["<main_collection>"].find_one({"<global_key>": <value>}, {"_id": 0, "<embed_field>": 1})
db["<reference_collection>"].find({"<global_key>": <value>}, {"_id": 0})
```

*Buffer — query the persistent MongoDB buffer collection:*
```python
db["buffer"].find({"<global_key>": <value>}, {"_id": 0})
# Also inspects the `unknown_top` sub-document for unclassified fields
```

**Step 4 — Result Merge**

All backend results are deep-merged into a single unified JSON document keyed by global key. The caller receives one clean response regardless of how many backends were queried.

---

### Insert Operation

**Input format:**
```json
{
  "operation": "insert",
  "data": {
    "<global_key>": <value>,
    "<scalar_field>": <value>,
    "<child_entity>": [{ "<child_field>": <value> }],
    "<unknown_field>": <value>
  }
}
```

**Step 1 — Flatten**  
Nested objects are expanded to dot-notation paths (`<object>.<subfield>`). Arrays are kept intact.

**Step 2 — Validate (Atomic)**  
- Type coercion attempted for each field (e.g. `"42"` → `42` for `int` fields)
- `not_null` fields verified — child field constraints only checked if their parent is present in the input
- `unique` fields queried against live backends before any write begins
- If any validation step fails, **nothing is written** — the whole operation aborts

**Step 3 — Route to Buckets**

```
Known field, storage_backend = SQL    →  sql_data
Known field, storage_backend = Mongo  →  mongo_data
Known field, storage_backend = Buffer →  buffer_data
Unknown field (not in metadata)       →  buffer_data["unknown_top"]
```

**Step 4 — Execute**

SQL runs in a single transaction (main table first, then child tables, FK injected into every child row). MongoDB uses `update_one` with `upsert=True` for embedded fields, and `insert_one` per item for reference collections. Unknown fields are written to the persistent MongoDB buffer collection under `unknown_top`.

---

### Update Operation — Delete + Insert Strategy

Update is implemented as a full **DELETE then INSERT** cycle. This ensures consistency across all backends without requiring separate partial-update logic for each backend type.

```
1. Read full existing record across all backends
2. Deep-merge old record with new data:
   - Nested objects: recursive field-level merge (unchanged sub-fields preserved)
   - Arrays: full replacement (consistent across SQL and MongoDB)
   - Global key always carried from the old record
3. Pre-validate merged record (unique check self-excludes the current record)
4. Delete the old record
5. Insert the merged record
6. Recovery path:
   - If insert fails after delete → re-insert old record
   - If recovery also fails → print full old record to terminal (no data loss)
```

---

### Delete Operation

Four delete modes, all metadata-driven:

| Mode | Trigger | Behaviour |
|---|---|---|
| Full record | `where` only (no `entity`) | Deletes from all backends in cascade order |
| Entity delete | `entity` + `where` | Deletes only from the owning backend |
| Field-wide delete | `field` key present | Removes that column/field across all records on all backends |
| Multi-record delete | `where.<global_key>` is a list | Iterates and deletes each record independently |

**Cascade order (Full Record Delete):**
1. SQL child tables — deleted first to satisfy FK constraints
2. SQL main table — deleted after all children
3. MongoDB reference collections — `delete_many` per collection
4. MongoDB main document — `delete_one`
5. MongoDB buffer collection — `delete_many`

All SQL deletes execute in a single transaction with rollback on any failure.

---

## 6. Performance Considerations

### How the design reduces query complexity and document rewriting

---

### 1. Metadata-Driven Query Planning

Because every field's backend, table, and collection name is pre-resolved in `metadata_store.json`, the query engine never performs schema discovery at query time. There are no `SHOW TABLES`, `describe`, or `listCollections` calls during CRUD. The routing table is consulted once per operation, in O(number of fields) time.

---

### 2. Selective Backend Querying

The read engine only contacts backends that actually hold the requested fields. If all requested fields are in SQL, MongoDB is never contacted. If only a reference collection is requested, SQLite is never queried. This avoids unnecessary network and I/O overhead proportional to the number of backends.

---

### 3. Indexed Foreign Keys

All child table foreign key columns carry explicit B-tree indexes:

```sql
CREATE INDEX idx_<child_table>_<global_key> ON <child_table>(<global_key>);
```

Without these, every JOIN requires a full table scan of the child table. With them, lookups are O(log n) regardless of table size.

---

### 4. SQL JOIN Fan-Out Prevention

When reading from multiple SQL tables, the system issues a single JOIN query rather than separate queries per table. This eliminates N+1 query patterns.

For 1:many child tables, array results are reconstructed in Python after the JOIN by grouping rows by the global key — avoiding the row-multiplication problem.

---

### 5. Two-Layer Buffer Architecture

**Staging buffer** (`buffer.json`, local file) is used exclusively during ingestion. Writing a batch of records to a local file is far faster than individual MongoDB insertions, which involve network round-trips.

After DB Initialization, only buffer-relevant fields (below frequency threshold or unknown) are flushed to the **persistent buffer** (MongoDB collection). Classified SQL and MongoDB fields are stripped out. This keeps the buffer collection small and CRUD buffer queries fast, with an index on the global key.

---

### 6. Embedding vs. Referencing for Write Performance

MongoDB's 16 MB BSON document size limit becomes a risk when arrays grow indefinitely. The configurable `AVG_SIZE_THRESHOLD` prevents this:

- Arrays with small average cardinality → embedded → single document read, no additional query
- Arrays with large average cardinality → separate reference collection → main document size stays bounded; the reference collection scales independently

Object fields (sub-documents) are always embedded, meaning a full record read returns all nested object data in a single MongoDB document fetch — zero additional queries.

---

### 7. Atomic SQL Transactions

All SQL inserts and deletes execute within a single `BEGIN / COMMIT` transaction. If any row fails (e.g., FK constraint violation), the entire operation is rolled back. This prevents partial writes that would create inconsistent state across related tables.

---

### 8. Automatic Reclassification — Adaptive Optimisation

As data volume grows, field frequencies naturally shift. Fields that initially go to the Buffer (low frequency) are automatically promoted to MongoDB or SQL when they become common. Fields that were once common but appear less frequently after bulk deletes can be demoted. This means:

- The system self-optimises over time without manual intervention
- Frequently-queried fields are always in the most appropriate backend for their access pattern
- No manual schema migration is ever required from the user

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
    — Influenced design decisions around the buffer holding area and adaptive reclassification.

---

**Streaming & API**

11. FastAPI Documentation. https://fastapi.tiangolo.com/  
    — Used for the simulation data server generating synthetic JSON records.

12. W3C. *Server-Sent Events Specification*. https://html.spec.whatwg.org/multipage/server-sent-events.html  
    — Protocol used for the real-time streaming ingestion pipeline (Phase 2).

---

*End of Report*
