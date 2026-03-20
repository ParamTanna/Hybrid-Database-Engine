# Code Generation Prompt — CS432 Assignment 2: Hybrid Database Framework

Generate a complete Python project from scratch in a new empty directory called `hybrid_framework/`.
Do not leave any file incomplete. Every function must be fully implemented — no stubs, no `pass`, no `# TODO`.
The system must be runnable end-to-end after `pip install -r requirements.txt`.

---

## Context

This is Assignment 2 of a Databases course project. Assignment 1 built a system that ingested JSON records from a stream, analyzed field statistics (frequency, type stability, uniqueness), and routed fields to either SQLite or MongoDB. Assignment 2 extends that by adding:
- User-defined schema registration (minimal — field names, types, simple constraints only)
- A proper buffer pipeline (JSON file, not SQLite)
- SQL normalization engine (multiple tables, PK/FK detection from data signals)
- MongoDB embed vs reference decisions (from data signals only)
- Metadata-driven CRUD query engine
- Interactive CLI loop as the entry point

---

## Two Processes During a Session

```
Terminal 1:  uvicorn simulation_api:app --port 8000   # data generator
Terminal 2:  python main.py                            # interactive framework CLI
```

The framework fetches records from `http://127.0.0.1:8000/record/{count}` via SSE when the user chooses to ingest. There is NO FastAPI layer in the framework itself.

---

## Complete Directory Structure

```
hybrid_framework/
├── main.py
├── simulation_api.py
├── config.py
├── schema_registry.py
├── buffer_manager.py
├── normalization_engine.py
├── mongo_strategy.py
├── metadata_manager.py
├── query_engine.py
├── crud.py
├── analysis.py
├── classification.py
├── ingest.py
├── data/                     (auto-created at runtime)
│   ├── hybrid.db
│   ├── buffer.json
│   └── metadata.json
└── requirements.txt
```

---

## requirements.txt

```
fastapi
uvicorn
faker
sse-starlette
sqlalchemy>=2.0
pymongo>=4.0
httpx>=0.25
```

---

## simulation_api.py

This is a standalone FastAPI data generator for a **university academic records system**.
It is completely independent of the framework. Run it separately on port 8000.

Use `faker` and `random` to generate realistic university records. Use `random.seed(42)` at the top.

Generate a pool of 500 student usernames at startup using `faker.user_name()`.
Generate a pool of 30 course codes (e.g. "CS101", "MA201") and course names at startup.
Generate a pool of 8 departments at startup (e.g. "Computer Science", "Mathematics", "Physics", etc.)

Each record represents one student activity snapshot. A record always contains `username`.

**Field pool with appearance weights** (use `random.uniform(0.05, 0.95)` per field, seeded):

Flat fields:
- `student_id`: integer 1000–9999 (tied to username, same username always gets same student_id — use a dict mapping username→student_id built at startup)
- `name`: faker full name (tied to username, same username always gets same name)
- `email`: faker email (tied to username)
- `age`: int 18–30
- `gpa`: float 4.0–10.0 rounded to 1 decimal. Introduce type drift: 10% of the time emit gpa as a string like "8.5" instead of float 8.5
- `department`: from department pool
- `year_of_study`: int 1–5
- `is_active`: bool
- `phone`: faker phone number
- `address`: nested dict `{"city": faker.city(), "state": faker.state(), "pincode": faker.postcode()}` — appears 70% of the time
- `cgpa_history`: list of 3–6 floats representing semester CGPAs — appears 50% of the time

Array fields (these are the repeating group candidates):
- `enrolled_courses`: list of dicts, each dict has `{"course_code": str, "course_name": str, "credits": int, "semester": str}`. List length 1–5. Appears 65% of the time.
- `submissions`: list of dicts, each dict has `{"assignment_id": str, "course_code": str, "submitted_at": str (ISO datetime), "score": float, "feedback": faker.sentence()}`. List length 1–8. Appears 50% of the time. Make this array occasionally long (up to 20 items, 15% of the time) to trigger the reference decision.
- `research_interests`: list of strings (faker words), length 1–4. Appears 40% of the time.

Introduce key ambiguity on `student_id`: 20% of the time emit it as `studentId` instead of `student_id`. (This tests that our system handles schema-declared names correctly even when the key name varies — the framework should normalize based on the registered schema.)

Introduce sparse/null fields: `phone` is None 30% of the time. `age` is missing entirely 25% of the time.

Endpoints:
- `GET /` → single record (JSON)
- `GET /record/{count}` → SSE stream, each event is `data: <json>\n\n`, sleep 0.005s between events

---

## config.py

```python
import os
from pathlib import Path

# --- Database ---
SQL_URL = os.environ.get("HYBRID_SQL_URL", "sqlite:///./data/hybrid.db")
MONGO_URI = os.environ.get("HYBRID_MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("HYBRID_MONGO_DB", "hybrid_db")

# --- File paths ---
DATA_DIR = Path("./data")
METADATA_FILE = DATA_DIR / "metadata.json"
BUFFER_FILE = DATA_DIR / "buffer.json"

# --- Classification thresholds (from Assignment 1) ---
FREQUENCY_THRESHOLD_SQL = 0.5
TYPE_STABILITY_THRESHOLD = 1.0

# --- Buffer ---
BUFFER_BATCH_SIZE = 50          # re-evaluate after this many new pending records
MIN_FIELD_OBSERVATIONS = 30     # minimum global observations before classifying a field

# --- Normalization engine ---
FD_SAMPLE_SIZE = 200            # max records to sample for functional dependency detection
FD_THRESHOLD = 0.95             # if B is determined by A in 95%+ of cases, it's an FD
MAX_INLINE_FIELDS = 5           # nested object with <= this many sub-fields gets flattened into parent table

# --- MongoDB strategy ---
MONGO_EMBED_MAX_ARRAY_LENGTH = 10   # avg array length above this → reference

# --- Join key ---
JOIN_KEY = "sys_ingested_at"
SECONDARY_JOIN_KEY = "username"
```

---

## analysis.py

Copy the following functions exactly from Assignment 1. These are reused without modification:
- `_value_type(v)`
- `analyze_buffer(records)` — takes a list of dicts, returns per-field stats dict
- `merge_cumulative_stats(prev_cumulative, batch_result, batch_size)` — merges batch into cumulative raw stats
- `cumulative_raw_to_derived(cumulative_raw)` — computes frequency, type_stability, etc. from raw cumulative

Remove the import of `flatten_value_for_type` from normalization. Instead define `flatten_value_for_type(value)` inline in this file — it just returns `value` as-is (nested dict/list are kept for has_nested detection).

Do NOT import from any normalization module.

---

## classification.py

Copy `classify_fields(field_stats)` exactly from Assignment 1.

It returns for each field: `{"backend": "sql"|"mongo"|"undecided", "unique": bool, "reason": str}`

Modify it as follows:
- Add a new check BEFORE the existing checks: if the field's global `presence_count` (from `field_stats[field]["presence_count"]`) is less than `MIN_FIELD_OBSERVATIONS` from config, return `{"backend": "undecided", "unique": False, "reason": "insufficient_observations"}` for that field. This means we haven't seen it enough times to classify it reliably.
- The JOIN_KEY (`sys_ingested_at`) is always `backend: "sql"`, `unique: True`, regardless of observation count.
- Import `FREQUENCY_THRESHOLD_SQL`, `TYPE_STABILITY_THRESHOLD`, `MIN_FIELD_OBSERVATIONS`, `JOIN_KEY` from `config`.
- Do NOT import JOIN_FIELDS. Use `JOIN_KEY` from config directly.

---

## ingest.py

This replaces `buffer.py` from Assignment 1. Key differences: no key normalization, no t_stamp.

```python
from datetime import datetime, timezone
from typing import Any


def _try_numeric(s: str) -> int | float | str:
    """If string is convertible to int or float, return that; else return original string."""
    # same logic as Assignment 1 buffer.py


def coerce_numeric_strings(record: dict[str, Any]) -> dict[str, Any]:
    """Convert string values to int/float when possible."""
    # same logic as Assignment 1 buffer.py, applied to top-level values only


def ingest_one(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Coerce numeric strings, then add sys_ingested_at.
    NO key normalization. NO t_stamp. Keys are used exactly as received.
    sys_ingested_at = datetime.now(timezone.utc).isoformat()
    """
```

---

## schema_registry.py

**Purpose:** Validate and store the user-defined schema. This is the first thing a user does in a session.

**Schema format the user provides (as a Python dict or JSON string):**

```json
{
  "fields": {
    "student_id":  {"type": "int",   "not_null": true, "unique": true},
    "username":    {"type": "str",   "not_null": true},
    "name":        {"type": "str",   "not_null": true},
    "gpa":         {"type": "float"},
    "department":  {"type": "str"},
    "is_active":   {"type": "bool"},
    "address":     {"type": "object"},
    "enrolled_courses": {"type": "array"},
    "submissions":      {"type": "array"},
    "research_interests": {"type": "array"},
    "cgpa_history":     {"type": "array"}
  }
}
```

Valid types: `"int"`, `"float"`, `"str"`, `"bool"`, `"array"`, `"object"`.
Valid constraints: `"not_null": true/false`, `"unique": true/false`.
No primary_key, no foreign_key, no embed/reference hints in the schema.

**Functions to implement:**

`validate_schema(schema_dict) -> tuple[bool, str]`
- Returns (True, "") if valid, (False, error_message) if not.
- Checks: "fields" key exists, each field has a "type", type is one of the valid types, constraints are booleans if present.

`register_schema(schema_dict) -> dict`
- Calls validate_schema. Raises ValueError if invalid.
- Stores the schema in metadata under key "schema" via metadata_manager.save_schema(schema_dict).
- Returns a summary dict: `{"fields_registered": int, "array_fields": [list of field names with type array], "object_fields": [list], "scalar_fields": [list]}`

`get_schema() -> dict`
- Loads and returns the schema from metadata. Returns {} if none registered.

`schema_field_names() -> set[str]`
- Returns the set of all field names declared in the schema.

---

## metadata_manager.py

**Purpose:** Single source of truth. Every decision made by every module is written here and read from here. Survives restarts.

`data/metadata.json` structure:

```json
{
  "schema": { "fields": { ... } },
  "cumulative_stats": {
    "total_records": 0,
    "fields": { }
  },
  "field_placement": {
    "student_id":  {"backend": "sql",   "table": "students",    "unique": true,  "reason": "stable_flat"},
    "gpa":         {"backend": "sql",   "table": "students",    "unique": false, "reason": "stable_flat"},
    "submissions": {"backend": "mongo", "collection": "submissions", "strategy": "reference", "reason": "nested_or_complex"},
    "address.city":{"backend": "sql",   "table": "students",    "unique": false, "reason": "stable_flat", "flattened_from": "address"},
    "some_rare":   {"backend": "undecided", "reason": "insufficient_observations"}
  },
  "sql_tables": {
    "students": {
      "columns": {
        "student_id": {"sql_type": "INTEGER", "unique": true,  "not_null": true, "primary_key": true},
        "username":   {"sql_type": "TEXT",    "unique": false, "not_null": true},
        "gpa":        {"sql_type": "REAL",    "unique": false, "not_null": false}
      },
      "primary_key": "student_id",
      "foreign_keys": []
    },
    "enrolled_courses": {
      "columns": { ... },
      "primary_key": "_row_id",
      "foreign_keys": [{"column": "student_id", "references_table": "students", "references_column": "student_id"}]
    }
  },
  "mongo_collections": {
    "submissions": {
      "strategy": "reference",
      "join_key": "sys_ingested_at",
      "parent_join_key": "sys_ingested_at",
      "fields": ["assignment_id", "course_code", "submitted_at", "score", "feedback"]
    },
    "main_documents": {
      "strategy": "embed",
      "embedded_fields": ["research_interests", "cgpa_history"],
      "fields": ["username", "sys_ingested_at", "research_interests", "cgpa_history"]
    }
  },
  "flattened_objects": {
    "address": ["address.city", "address.state", "address.pincode"]
  },
  "total_records_seen": 0
}
```

**Functions to implement:**

`load() -> dict` — load metadata.json, return {} if missing or corrupted.

`save(data: dict) -> None` — write metadata.json atomically (write to temp file, rename).

`save_schema(schema_dict: dict) -> None` — update "schema" key and save.

`get_schema() -> dict` — return metadata["schema"] or {}.

`save_cumulative_stats(cumulative_raw: dict, total: int) -> None`

`get_cumulative_stats() -> dict`

`save_field_placement(placement: dict) -> None` — merge new placement decisions into existing "field_placement", save.

`get_field_placement() -> dict`

`get_placement_for_field(field_name: str) -> dict | None`

`save_sql_tables(tables: dict) -> None` — merge into "sql_tables", save.

`get_sql_tables() -> dict`

`save_mongo_collections(collections: dict) -> None` — merge into "mongo_collections", save.

`get_mongo_collections() -> dict`

`save_flattened_objects(flattened: dict) -> None` — merge into "flattened_objects", save.

`get_flattened_objects() -> dict` — returns mapping of original field name → list of dot-notation column names.

`increment_total_records(n: int) -> None`

`get_total_records() -> int`

`reset() -> None` — delete metadata.json and buffer.json, reset everything.

---

## buffer_manager.py

**Purpose:** Hold unclassified field values keyed by `sys_ingested_at`. Use a JSON file (`data/buffer.json`), NOT SQLite.

**`data/buffer.json` structure:**

```json
{
  "pending_fields": {
    "2026-03-18T10:00:01.123456+00:00": {
      "some_rare_field": "value1",
      "another_sparse": 42
    },
    "2026-03-18T10:00:01.456789+00:00": {
      "some_rare_field": null,
      "another_sparse": 17
    }
  },
  "new_since_last_eval": 0
}
```

`pending_fields` maps `sys_ingested_at` → dict of {field_name: value} for ONLY the fields of that record that are currently `undecided`. When a field gets classified, its values are removed from all entries in `pending_fields` and committed to the correct backend.

`new_since_last_eval` is incremented on every `add_pending_fields` call. When it reaches `BUFFER_BATCH_SIZE`, a re-evaluation is triggered automatically.

**Functions to implement:**

`load_buffer() -> dict` — load buffer.json, return `{"pending_fields": {}, "new_since_last_eval": 0}` if missing.

`save_buffer(data: dict) -> None`

`add_pending_fields(sys_ingested_at: str, undecided_fields: dict) -> None`
- Adds the undecided field values for this record to `pending_fields[sys_ingested_at]`.
- Increments `new_since_last_eval`.
- If `new_since_last_eval >= BUFFER_BATCH_SIZE`: calls `trigger_reevaluation()`.

`trigger_reevaluation() -> dict`
- Collects all values across all `pending_fields` entries and reconstructs a list of mini-records (one per sys_ingested_at timestamp, containing only the pending fields for that record).
- Calls `analysis.analyze_buffer(mini_records)` to get batch stats for pending fields only.
- Loads cumulative stats from metadata_manager.
- Calls `analysis.merge_cumulative_stats(prev_cumulative, batch_stats, len(mini_records))` to get merged raw stats.
- Saves merged cumulative stats to metadata.
- Calls `analysis.cumulative_raw_to_derived(merged_raw)` to get derived stats.
- Calls `classification.classify_fields(derived_stats)`.
- For each field now classified as "sql" or "mongo": calls `commit_classified_field(field_name, backend, decisions[field_name])`.
- Resets `new_since_last_eval` to 0. Saves buffer.
- Returns a dict summarizing what was classified and what remains undecided.

`commit_classified_field(field_name: str, decision: dict) -> None`
- Iterates over all entries in `pending_fields`.
- For each entry that contains `field_name`: extracts the value, and calls `crud.insert_pending_field_value(sys_ingested_at, field_name, value, decision)` to write it to the correct backend.
- Removes `field_name` from that entry in `pending_fields`.
- After processing all entries, removes any `pending_fields` entry that is now an empty dict.
- Saves buffer.
- Updates `field_placement` in metadata for this field.

`force_flush() -> dict`
- For every field still present in any `pending_fields` entry: force-classify it as MongoDB (safe fallback).
- Calls `commit_classified_field` for each such field with backend="mongo".
- Clears `pending_fields` entirely. Saves buffer.
- Returns summary of what was flushed.

`get_pending_field_names() -> set[str]`
- Returns the set of all distinct field names currently present in any pending_fields entry.

`get_buffer_stats() -> dict`
- Returns: `{"pending_record_count": int, "pending_field_names": list, "new_since_last_eval": int}`

---

## normalization_engine.py

**Purpose:** Decide how many SQL tables to create, which fields go where, and what the PKs and FKs are. All decisions come from data signals and schema constraints — nothing is hardcoded.

**Input:** `field_stats` (cumulative derived stats from analysis), `schema` (from schema_registry), `classified_sql_fields` (list of field names classified as SQL by classification.py), `all_ingested_records` (list of all records seen so far — needed for FD detection).

**Output:** A dict describing the SQL table structure, written to metadata via `metadata_manager.save_sql_tables()`.

**Implement these functions:**

### `detect_entity_identifier(field_stats, schema) -> str | None`

Finds the most likely primary key among SQL-classified fields.

Rules in priority order:
1. A field declared `unique: true` AND `not_null: true` in the schema, whose `uniqueness_ratio` in field_stats is >= 0.99 → strong PK candidate.
2. Among multiple candidates from rule 1, prefer the one with `dominant_type == "int"` (integer IDs are cleaner PKs).
3. If no field passes rule 1 but a field has `uniqueness_ratio >= 0.99` in the data alone → weaker candidate.
4. If nothing qualifies → return None (a surrogate `_row_id` INTEGER PRIMARY KEY AUTOINCREMENT will be used).

### `detect_functional_dependencies(records, sql_fields, sample_size) -> dict[str, set[str]]`

Detects which fields are functionally determined by other fields.

Algorithm:
- Sample up to `sample_size` records from `records` that contain at least two SQL fields.
- For each pair of SQL fields (A, B): build a dict mapping each distinct value of A to the set of distinct values of B that co-occur with it across all sampled records.
- If for every value of A, the set of B values has exactly 1 element (i.e. max(len(v) for v in mapping.values()) == 1), then B is functionally dependent on A: FD A → B.
- Use `FD_THRESHOLD` from config: if the proportion of A-values that uniquely determine B is >= FD_THRESHOLD, count it as an FD.
- Returns a dict: `{A: set_of_fields_determined_by_A}`.
- Example: if `username` always determines `name` and `email`, returns `{"username": {"name", "email"}}`.

### `detect_repeating_groups(schema, field_stats) -> list[dict]`

Finds fields that represent one-to-many relationships and should become child tables.

A field is a repeating group if ANY of these are true:
1. Its type in the schema is `"array"` AND field_stats shows `dominant_type == "list"` — it's an array field observed as lists in data.
2. Its type in schema is `"object"` AND field_stats shows `dominant_type == "list"` — schema said object but data shows it's actually a list.
3. The field has `has_nested == True` in field_stats AND `dominant_type == "list"`.

For each detected repeating group field, inspect the actual array items (from a sample of ingested records) to determine:
- What sub-fields appear inside each array item? These become columns of the child table.
- Are the sub-fields themselves stable and scalar (suitable for SQL)? If yes → child table in SQL. If sub-fields are themselves nested → MongoDB.

Returns list of dicts: `[{"parent_field": str, "child_table_name": str, "child_fields": dict_of_subfield_stats, "goes_to_sql": bool}]`

### `detect_nested_objects(schema, field_stats) -> list[dict]`

Finds fields typed as `"object"` in schema that are observed as dicts in data (`dominant_type == "dict"`).

For each such field, determine:
- How many distinct sub-keys appear inside it across sampled records?
- Are all sub-values scalar?
- If sub-key count <= `MAX_INLINE_FIELDS` AND all sub-values are scalar → inline (flatten into parent table using dot-notation column names like `address.city`).
- Otherwise → separate linked SQL table (if sub-fields are stable and frequent) or MongoDB.

Returns list of dicts: `[{"field": str, "strategy": "inline"|"separate_table"|"mongo", "sub_fields": list[str]}]`

### `build_sql_table_schema(sql_fields, field_stats, schema, pk_field, fd_groups, repeating_groups, nested_objects) -> dict`

Assembles the full SQL table schema dict that gets written to metadata.

Logic:
1. Start with a "main" table. Its name is derived from the schema context (default: "records" if no clear entity name, or the name of the PK field's entity context).
2. All scalar SQL fields that are NOT part of a repeating group and NOT a flattened nested object go into the main table.
3. For each inline nested object: its dot-notation sub-fields go into the main table (e.g. `address.city` becomes a column).
4. For each repeating group that goes to SQL: create a child table. Add the parent's PK as a FK column in the child table (NOT NULL). Add `sys_ingested_at` to the child table as well (for joining with MongoDB).
5. PK assignment:
   - Main table: `pk_field` if detected, else `_row_id` INTEGER PRIMARY KEY AUTOINCREMENT.
   - Child tables: `_row_id` INTEGER PRIMARY KEY AUTOINCREMENT always.
6. `sys_ingested_at` TEXT UNIQUE goes into the main table always (it's the universal join key).
7. `username` TEXT goes into every table always (secondary join key).
8. Column SQL types: map from dominant_type: int→INTEGER, float→REAL, bool→INTEGER, str→TEXT, anything else→TEXT.
9. Apply `not_null` and `unique` constraints from schema to main table columns.

Returns:
```python
{
  "main_table_name": "records",   # or derived name
  "tables": {
    "records": {
      "columns": {
        "student_id": {"sql_type": "INTEGER", "primary_key": True, "unique": True, "not_null": True},
        "sys_ingested_at": {"sql_type": "TEXT", "unique": True, "not_null": True},
        "address.city": {"sql_type": "TEXT", "unique": False, "not_null": False}
      },
      "primary_key": "student_id",
      "foreign_keys": []
    },
    "enrolled_courses": {
      "columns": {
        "_row_id": {"sql_type": "INTEGER", "primary_key": True},
        "course_code": {"sql_type": "TEXT"},
        "course_name": {"sql_type": "TEXT"},
        "credits": {"sql_type": "INTEGER"},
        "semester": {"sql_type": "TEXT"},
        "student_id": {"sql_type": "INTEGER", "not_null": True},
        "sys_ingested_at": {"sql_type": "TEXT", "not_null": True},
        "username": {"sql_type": "TEXT"}
      },
      "primary_key": "_row_id",
      "foreign_keys": [{"column": "student_id", "references_table": "records", "references_column": "student_id"}]
    }
  },
  "flattened_objects": {
    "address": ["address.city", "address.state", "address.pincode"]
  }
}
```

### `run_normalization(all_records, field_stats, schema) -> dict`

Top-level function called by the pipeline. Calls all the above in order, writes results to metadata, returns the table schema dict.

---

## mongo_strategy.py

**Purpose:** For all fields/structures classified as MongoDB, decide whether each should be embedded in the main document or stored in a separate referenced collection.

**Input:** `mongo_fields` (list of field names classified as mongo), `field_stats`, `schema`, `sample_records` (list of actual ingested records for observing array lengths and update patterns).

**Implement these functions:**

### `_avg_array_length(field_name, sample_records) -> float`
Samples records that contain `field_name` and computes the average length of the list value. Returns 0.0 if field never appears or is not a list.

### `_items_contain_nested_arrays(field_name, sample_records) -> bool`
Checks if any array item in `field_name` is itself a list. Returns True if found in any sampled record.

### `_is_shared_structure(field_name, schema, field_stats) -> bool`
Heuristic: if the sub-field names of this array's items overlap significantly (>50% overlap) with the sub-field names of another array field in the schema, this structure is likely a shared entity. Returns True if shared.

### `_is_update_heavy(field_name, sample_records) -> bool`
Heuristic: group sample_records by username. For each username, check if the `field_name` array value changes across records with the same username (different `sys_ingested_at`). If in >30% of usernames the array value changes across records, it is update-heavy. Returns True if update-heavy.

### `decide_strategy(field_name, field_stats, schema, sample_records) -> dict`

Returns `{"strategy": "embed"|"reference", "reason": str}`.

Decision rules in order (first matching rule wins):

1. If field is a scalar list (dominant sub-type is not dict) — e.g. `research_interests: ["ml", "db"]` → **embed**. Reason: "scalar_array".
2. If field is a dict (not a list) → **embed**. Reason: "subdocument".
3. If `_avg_array_length > MONGO_EMBED_MAX_ARRAY_LENGTH` → **reference**. Reason: "large_array".
4. If `_items_contain_nested_arrays` → **reference**. Reason: "nested_arrays_in_items".
5. If `_is_shared_structure` → **reference**. Reason: "shared_entity".
6. If `_is_update_heavy` → **reference**. Reason: "update_heavy".
7. Default → **embed**. Reason: "small_stable_array".

### `build_mongo_collection_schema(mongo_fields, field_stats, schema, sample_records) -> dict`

For each mongo field:
- Call `decide_strategy`.
- If embed: add to `"main_documents"` collection schema under `"embedded_fields"`.
- If reference: create a separate collection named after the field (e.g. `"submissions"`). Determine its sub-fields from sampling actual array items in `sample_records`. Collection schema includes `sys_ingested_at` as join key.

Returns:
```python
{
  "main_documents": {
    "strategy": "embed",
    "embedded_fields": ["research_interests", "cgpa_history"],
    "fields": ["username", "sys_ingested_at", "research_interests", "cgpa_history"]
  },
  "submissions": {
    "strategy": "reference",
    "join_key": "sys_ingested_at",
    "fields": ["assignment_id", "course_code", "submitted_at", "score", "feedback", "sys_ingested_at", "username"],
    "reason": "large_array"
  }
}
```

Writes result to metadata via `metadata_manager.save_mongo_collections()`. Returns the collection schema dict.

### `run_mongo_strategy(mongo_fields, field_stats, schema, sample_records) -> dict`
Top-level function. Calls `build_mongo_collection_schema`. Returns the result.

---

## crud.py

**Purpose:** Execute SQL and MongoDB operations. Only module that directly touches SQLite and MongoDB. Handles table creation, insertion, reading, deletion, and result merging.

**SQL engine:** SQLAlchemy with `create_engine(SQL_URL)`. Use `text()` for all queries.
**MongoDB:** PyMongo `MongoClient`. If MongoDB is unavailable (connection error), catch the exception, print a warning to stderr, and continue — SQL still works.

### SQL table management

`ensure_all_tables(sql_tables: dict) -> None`
- For each table in `sql_tables` dict from metadata: if table doesn't exist, run CREATE TABLE with all columns, PK, UNIQUE constraints.
- If table exists: ADD COLUMN for any new columns not yet present. DROP COLUMN for any column no longer in the schema.
- Use SQLite PRAGMA table_info to inspect existing columns.
- Column definition: `"column_name" SQL_TYPE [NOT NULL] [UNIQUE]`. For PRIMARY KEY: `"col" SQL_TYPE PRIMARY KEY`. For AUTOINCREMENT: `"_row_id" INTEGER PRIMARY KEY AUTOINCREMENT`.
- For FK columns: after table creation, SQLite doesn't enforce FKs natively but add a comment in the DDL for documentation.

`_value_to_sql(value: Any) -> Any`
- None → None
- dict or list → `json.dumps(value)`
- bool → 1 or 0
- else → value as-is

### Insertion

`insert_record(record: dict, sql_tables: dict, mongo_collections: dict, field_placement: dict, flattened_objects: dict) -> None`

This is the main insert function. It splits one normalized record across all backends.

Steps:
1. Determine the main table name from `sql_tables` metadata.
2. Build the main table row: for each SQL field in the main table, extract its value from the record. For flattened objects (e.g. `address`): unpack `record["address"]` into `{"address.city": ..., "address.state": ..., "address.pincode": ...}` using the flattened_objects mapping.
3. Run `INSERT OR REPLACE INTO main_table (...) VALUES (...)` with the row.
4. For each child SQL table (repeating group tables): extract the array from the record (e.g. `record["enrolled_courses"]`), iterate over each item, add the parent PK value and `sys_ingested_at` and `username` to each item dict, run INSERT for each item.
5. For MongoDB embedded fields: build the main document with `username`, `sys_ingested_at`, and all embedded fields. Insert into `main_documents` collection.
6. For MongoDB reference fields: for each reference collection, extract the array from the record, iterate over each item, add `sys_ingested_at` and `username`, insert each item as a separate document in the collection.

`insert_pending_field_value(sys_ingested_at: str, field_name: str, value: Any, decision: dict) -> None`
- Called by buffer_manager when a previously-undecided field gets classified.
- If backend is "sql": UPDATE the appropriate table SET field_name = value WHERE sys_ingested_at = sys_ingested_at.
- If backend is "mongo": use MongoDB update_one with $set on the document matching sys_ingested_at in the appropriate collection.

### Reading

`execute_read(operation: dict, sql_tables: dict, mongo_collections: dict, field_placement: dict, flattened_objects: dict) -> list[dict]`

`operation` has keys: `"filters"` (dict of field→value constraints), `"fields"` (list of field names to return, or None for all).

Steps:
1. Determine which fields go to SQL vs MongoDB from `field_placement`.
2. For SQL fields: determine which tables contain them. If fields span multiple tables, JOIN on the FK relationship. Build a parameterized SELECT query with WHERE clauses from `filters`. Execute and collect rows as list of dicts.
3. For MongoDB fields: for each collection, run `find()` with the filter (translate `filters` to MongoDB filter dict using `username` or `sys_ingested_at`). Collect documents.
4. Reconstruct nested objects: for any SQL row, find columns with `.` in their name (e.g. `address.city`, `address.state`), group by prefix, rebuild nested dict (e.g. `{"address": {"city": ..., "state": ...}}`), remove the dot-notation keys from the row.
5. Merge SQL results and MongoDB results on `sys_ingested_at`: for each `sys_ingested_at` value, combine the SQL row dict and the MongoDB document dict into one flat dict. Remove duplicate `username` and `sys_ingested_at` keys.
6. If `operation["fields"]` is specified, filter the merged dict to only those keys before returning.
7. Return list of merged dicts.

### Deletion

`execute_delete(operation: dict, sql_tables: dict, mongo_collections: dict) -> dict`

`operation` has key: `"filters"`.

Steps:
1. Delete from child SQL tables first (to avoid FK violations): for each child table, DELETE WHERE the FK value matches (find FK value by first querying the main table with the filter).
2. Delete from main SQL table WHERE filter matches.
3. Delete from all MongoDB collections WHERE filter matches (using `delete_many`).
4. Returns `{"deleted_sql_rows": int, "deleted_mongo_docs": int}`.

### Update (delete + reinsert)

`execute_update(operation: dict, sql_tables, mongo_collections, field_placement, flattened_objects, schema) -> dict`

`operation` has keys: `"filters"`, `"set"` (dict of field→new_value).

Steps:
1. Read the full current record using `execute_read` with the same filters and fields=None.
2. If no record found, return `{"updated": 0}`.
3. For each record found: merge the `"set"` values into the record dict (overwrite matching keys).
4. Delete the old record using `execute_delete`.
5. Re-ingest the merged record using `insert_record` (it already has a `sys_ingested_at` — keep it, do not generate a new one).
6. Returns `{"updated": int}`.

---

## query_engine.py

**Purpose:** Parse a user's JSON CRUD operation, validate it, call the appropriate crud.py function, and return the formatted result.

**JSON operation format:**

```json
{"operation": "read",   "filters": {"username": "rahul_21"}, "fields": ["username", "gpa", "submissions"]}
{"operation": "insert", "record": {"student_id": 3, "username": "arun_05", "gpa": 9.1, ...}}
{"operation": "delete", "filters": {"username": "rahul_21"}}
{"operation": "update", "filters": {"username": "rahul_21"}, "set": {"gpa": 8.9}}
```

`handle_query(operation_json: dict) -> dict`

- Validates operation type is one of: read, insert, delete, update.
- Loads required metadata (sql_tables, mongo_collections, field_placement, flattened_objects) from metadata_manager.
- Dispatches to the correct crud.py function.
- For insert: passes the record through `ingest.ingest_one()` first, then to `buffer_manager` for the undecided fields and to `crud.insert_record` for the decided fields.
- Returns result as a dict with a `"status"` key ("ok" or "error") and a `"data"` key for read results or a `"summary"` key for write results.

`_validate_filters(filters: dict, schema: dict) -> tuple[bool, str]`
- Checks that all filter keys are declared in the schema.

---

## main.py

**Purpose:** Interactive CLI entry point. Startup checks, then menu loop.

**Startup sequence:**
1. Print ASCII header:
```
╔══════════════════════════════════════════╗
║   Hybrid Database Framework — CS432     ║
║   SQL + MongoDB Adaptive Ingestion      ║
╚══════════════════════════════════════════╝
```
2. Create `data/` directory if it doesn't exist.
3. Check if `data/metadata.json` is readable (warn if corrupted).
4. Try connecting to SQLite (just `create_engine(SQL_URL).connect()` — if it fails, print error and exit).
5. Try connecting to MongoDB (`MongoClient(MONGO_URI).server_info()` with timeout 2s — if it fails, print "Warning: MongoDB unavailable. SQL-only mode." and continue).
6. Check if the simulation API is reachable: `GET http://127.0.0.1:8000/` with a 2s timeout. If unreachable, print "Warning: Simulation API not reachable at port 8000. Ingestion will fail." and continue.
7. Show current session status: schema registered? yes/no. Total records seen. Buffer pending count.

**Main menu loop:**

```
═══════════════════════════════
  Main Menu
═══════════════════════════════
  [1] Register / update schema
  [2] Ingest records from stream
  [3] Query (CRUD operations)
  [4] View placement metadata
  [5] Flush buffer
  [6] Reset all data
  [0] Exit
```

**Option 1 — Register schema:**
- Print instructions for the schema format.
- Ask: "Enter schema as JSON, or type 'file' to load from a .json file path:"
- If user types 'file': ask for file path, read and parse JSON.
- Otherwise: read multi-line input until user enters a line containing only "END".
- Call `schema_registry.register_schema(parsed_dict)`.
- Print the summary returned.
- Then immediately call `normalization_engine.run_normalization` and `mongo_strategy.run_mongo_strategy` on any already-ingested data (or with empty records if none yet), and print what table structure was determined.
- Call `crud.ensure_all_tables(metadata_manager.get_sql_tables())`.

**Option 2 — Ingest records:**
- Ask: "How many records to fetch from stream? (default 100): "
- Fetch records from `http://127.0.0.1:8000/record/{count}` using SSE (same `fetch_stream_sse` logic from Assignment 1's ingestion.py — httpx if available, else urllib).
- For each raw record: call `ingest.ingest_one(raw)`.
- Run `analysis.analyze_buffer(batch)` on the batch.
- Merge into cumulative stats via `analysis.merge_cumulative_stats`.
- Save cumulative stats to metadata.
- Run `analysis.cumulative_raw_to_derived` on merged stats.
- Run `classification.classify_fields(derived_stats)`.
- Separate fields into: sql_decided, mongo_decided, undecided.
- For sql_decided and mongo_decided fields: if this is the first time classifying (no sql_tables in metadata yet), run `normalization_engine.run_normalization` and `mongo_strategy.run_mongo_strategy` with the current batch records, build table schemas, save to metadata, call `crud.ensure_all_tables`.
- If sql_tables already exist in metadata: check if any new fields were just classified — if so, update table schemas (add new columns).
- For each ingested record: call `crud.insert_record(record, ...)` for all decided fields. For undecided fields: call `buffer_manager.add_pending_fields(sys_ingested_at, undecided_field_values)`.
- After processing all records: print a summary table:
```
Ingested 100 records.

Field Placement Summary:
Field               Backend    Table/Collection     Reason
------------------  ---------  -------------------  ----------------
student_id          SQL        records              stable_flat
username            SQL        records              join_key
gpa                 SQL        records              stable_flat
address.city        SQL        records              stable_flat
address.state       SQL        records              stable_flat
enrolled_courses    SQL        enrolled_courses     repeating_group
submissions         MongoDB    submissions          large_array (ref)
research_interests  MongoDB    main_documents       scalar_array (emb)
some_rare_field     BUFFER     —                    insufficient_obs

Buffer status: 3 fields pending across 87 records.
```

**Option 3 — Query (CRUD):**

Sub-menu:
```
  [a] Read
  [b] Insert
  [c] Delete
  [d] Update
  [e] Raw JSON operation
  [b] Back
```

For read/insert/delete/update: prompt the user for each required field interactively (e.g. "Filter field name:", "Filter value:", "Fields to return (comma-separated, or Enter for all):").
Construct the operation dict, call `query_engine.handle_query(op)`, pretty-print the result as JSON.

For raw JSON: ask user to paste a JSON operation dict (multi-line, END to finish), parse and pass to `query_engine.handle_query`.

**Option 4 — View metadata:**
Pretty-print the full metadata dict from `metadata_manager.load()` using `json.dumps(..., indent=2)`.

**Option 5 — Flush buffer:**
- Ask: "Are you sure? This will force all buffered fields to MongoDB. (y/n): "
- Call `buffer_manager.force_flush()`.
- Print summary.
- Ensure SQL tables are updated if any buffer-flushed fields ended up in SQL (won't happen with force_flush since they all go mongo, but check).

**Option 6 — Reset all data:**
- Ask: "Are you sure? This deletes ALL data, metadata, and buffer. (y/n): "
- Call `metadata_manager.reset()`.
- Drop all SQLite tables (connect and run `DROP TABLE IF EXISTS` for each table in metadata, then delete the .db file).
- Drop all MongoDB collections.
- Print "All data cleared. Schema, buffer, and database reset."

**Option 0 — Exit:**
- Print "Goodbye."
- `sys.exit(0)`

---

## Cross-Cutting Requirements

### Error handling
- Every function that touches a file (metadata.json, buffer.json) must use try/except for JSONDecodeError and OSError.
- Every function that touches MongoDB must catch `pymongo.errors.ConnectionFailure` and `pymongo.errors.ServerSelectionTimeoutError` — print warning to stderr and return a safe default (empty list, False, etc.).
- Every function that touches SQLAlchemy must catch `sqlalchemy.exc.OperationalError` and `sqlalchemy.exc.ProgrammingError`.

### `sys_ingested_at` as universal join key
- This key is present in EVERY SQL table (every table has a `sys_ingested_at TEXT` column).
- This key is present in EVERY MongoDB document (main_documents and all reference collections).
- All merging of SQL + MongoDB results happens exclusively on this key.
- When reconstructing a full record from multiple tables/collections, `sys_ingested_at` is the foreign key used for all joins.

### Dot-notation column reconstruction
- Any SQL column whose name contains a `.` was flattened from a nested object.
- The `flattened_objects` metadata records which original field names map to which dot-notation columns.
- When returning read results to the user, ALWAYS reconstruct: group dot-notation columns by their prefix, rebuild the nested dict, replace the flat columns with the nested structure in the returned JSON.
- Example: SQL columns `address.city = "Ahmedabad"` and `address.state = "Gujarat"` become `"address": {"city": "Ahmedabad", "state": "Gujarat"}` in the returned JSON.

### Schema field names vs stream field names
- The simulation_api sometimes emits `studentId` instead of `student_id`. The framework must handle this gracefully. In `ingest.ingest_one()`, after coercing numeric strings, apply a normalization step that maps incoming field names to their schema-declared canonical names. Use the schema from `schema_registry.get_schema()` to build a case-insensitive lookup: `{name.lower().replace("_","").replace("-",""): canonical_name for canonical_name in schema["fields"]}`. Apply this mapping to each incoming record's keys. This replaces the Assignment 1 normalization module — it is schema-aware normalization, not synonym-based.

### MongoDB document structure
- Main documents collection stores: `username`, `sys_ingested_at`, and all embedded field values.
- Reference collections store: each array item as a separate document plus `username` and `sys_ingested_at` from the parent.
- All MongoDB inserts use `insert_one`.
- All MongoDB reads use `find` with appropriate projection.

### The `all_records` store for FD detection
- The normalization engine needs access to all ingested records to run FD detection.
- Store a rolling sample of the last `FD_SAMPLE_SIZE` records in memory in `main.py` as a module-level list `_record_sample`. Pass this to `normalization_engine.run_normalization` when called.
- This list is not persisted to disk — it's rebuilt from scratch on each session.

### No circular imports
Module dependency order (each module only imports from modules listed to its right):
```
main.py → query_engine, buffer_manager, normalization_engine, mongo_strategy, crud, metadata_manager, schema_registry, analysis, classification, ingest, config
query_engine → crud, metadata_manager, schema_registry, ingest, buffer_manager, config
crud → metadata_manager, config
buffer_manager → analysis, classification, metadata_manager, config
normalization_engine → metadata_manager, config
mongo_strategy → metadata_manager, config
metadata_manager → config
schema_registry → metadata_manager, config
analysis → config
classification → config
ingest → config
```

---

## Final Notes

- All user-facing output must be clean and readable in a terminal. Use `─`, `═`, `│` box-drawing characters for tables and headers.
- All JSON input from the user must be parsed with `json.loads()` with a clear error message if parsing fails.
- The project must work even if MongoDB is not running (SQL-only mode with warnings).
- The project must work for multiple sequential ingestion runs in the same session — cumulative stats must accumulate correctly across multiple "Option 2" calls.
- Never hardcode field names anywhere except `sys_ingested_at`, `username`, and `_row_id`. All other field names are discovered dynamically from the schema and data.
- Write proper Python docstrings for every function.
- Use type hints throughout.
