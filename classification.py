"""
classification.py  -  Phases 3 + 4 + 5 + 6
============================================

Phase 3  -  Field Analysis
    frequency = occurrence_count / total_records
    frequency < 10%  ->  stays in Buffer (not enough data)

Phase 4  -  Storage Classification
    Only top-level entities are classified; children inherit recursively.

    CASE 1  Primitive  (int / float / string / boolean)
        freq > 50%          ->  SQL main table
        10% <= freq <= 50%  ->  Mongo document
        freq < 10%          ->  Buffer

    CASE 2  Nested  (object / array)
        independent_query = true          ->  SQL child table  (strongest rule)
        else if array:
            appendable = true:
                avg_size = total_elements / occurrence_count
                avg_size <= AVG_SIZE_THRESHOLD (5)  ->  Mongo embed
                avg_size >  AVG_SIZE_THRESHOLD (5)  ->  Mongo reference
            appendable = false              ->  SQL child table
                (arrays are inherently one-to-many; one_to_many is auto-set)
        else (object):
            ->  Mongo embed

    CASE 3  Buffer
        low_confidence / unknown, or freq < 10%

Phase 5  -  Key Management
    SQL main table   ->  global_key is PRIMARY KEY
    SQL child table  ->  global_key is FOREIGN KEY
    Mongo reference  ->  global_key stored in document
    Mongo embed      ->  no key needed
    Buffer           ->  global_key always attached

Phase 6  -  Final Storage Mapping
    Writes storage_map.json
    Updates metadata_store.json  (storage_backend + storage_detail)

Run:
    python classification.py
"""

import json
import os
import sys

METADATA_FILE      = "metadata_store.json"
AVG_SIZE_THRESHOLD = 5      # avg array elements per record; above this -> reference
FREQ_RARE          = 0.10    # < 10%  ->  Buffer
FREQ_SQL           = 0.50    # > 50%  ->  SQL

# Common suffixes to strip when deriving the main table name from global_key
# e.g. "customer_id" -> "customers",  "user_pk" -> "users"
_KEY_SUFFIXES = ("_id", "_key", "_pk", "_uuid", "_code", "_no", "_num")

PRIMITIVE_TYPES = {"int", "float", "string", "boolean"}


# ---------------------------------------------------------------------------
# Load / save
# ---------------------------------------------------------------------------

def _load(path: str) -> dict:
    if not os.path.exists(path):
        sys.exit(f"[ERROR] {path} not found - run previous phases first.")
    with open(path, "r") as f:
        return json.load(f)


def _save(path: str, data: dict):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _main_table_name(global_key: str) -> str:
    """
    Derive the SQL main table name purely from the global_key value.
    Strips known key suffixes, then pluralises.
    Works for any schema - no field names are hardcoded.
    """
    name = global_key.lower()
    for suffix in _KEY_SUFFIXES:
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    name = name.rstrip("_")
    if name.endswith("y") and not name.endswith(("ay", "ey", "iy", "oy", "uy")):
        return name[:-1] + "ies"
    if name.endswith(("s", "sh", "ch", "x", "z")):
        return name + "es"
    return name + "s"


def _propagate(fname: str, backend: str, detail: str, fields: dict):
    """
    Recursively propagate a storage decision from a parent to ALL descendants,
    regardless of nesting depth - no hardcoded level limits.
    """
    for child in fields.get(fname, {}).get("children", []):
        if child in fields:
            fields[child]["storage_backend"] = backend
            fields[child]["storage_detail"]  = detail
            _propagate(child, backend, detail, fields)


# ---------------------------------------------------------------------------
# Phase 3  -  Frequency calculation
# ---------------------------------------------------------------------------

def phase3_field_analysis(meta: dict) -> dict:
    """Compute frequency for every field. Returns {field_name: frequency}."""
    total    = meta["total_records"]
    freq_map = {}

    for fname, fdata in meta["fields"].items():
        freq = (fdata["occurrence_count"] / total) if total > 0 else 0.0
        fdata["frequency"] = round(freq, 4)
        freq_map[fname]    = freq

    print("\n" + "=" * 60)
    print("  PHASE 3  -  FIELD ANALYSIS")
    print("=" * 60)
    print(f"  Total records : {total}")
    print(f"  Rare threshold: frequency < {FREQ_RARE*100:.0f}%")
    print("-" * 60)
    for fname, freq in sorted(freq_map.items(), key=lambda x: -x[1]):
        flag = "  [RARE -> Buffer]" if freq < FREQ_RARE else ""
        print(f"  {fname:<30}  {freq*100:5.1f}%{flag}")
    print("=" * 60)

    return freq_map


# ---------------------------------------------------------------------------
# Phase 4  -  Storage Classification
# ---------------------------------------------------------------------------

def _classify_entity(fname: str, fdata: dict, freq: float,
                     global_key: str) -> tuple[str, str]:
    """
    Returns (storage_backend, storage_detail) for one TOP-LEVEL entity.
    All logic driven purely by metadata flags - no field names hardcoded.
    """
    # Unknown / low-confidence fields always go to Buffer
    if fdata.get("confidence") == "low":
        return "Buffer", "Buffer"

    # Rare fields always go to Buffer regardless of type
    if freq < FREQ_RARE:
        return "Buffer", "Buffer"

    field_type = fdata["type"]

    # CASE 1: Primitive
    if field_type in PRIMITIVE_TYPES:
        if fname == global_key or freq > FREQ_SQL:
            table = _main_table_name(global_key)
            return "SQL", f"SQL.{table}"
        else:
            return "Mongo", "Mongo.document"

    # CASE 2: Nested (object / array)
    if field_type in ("object", "array"):

        # Strongest rule: independent_query = true -> SQL child table
        if fdata.get("independent_query") is True:
            return "SQL", f"SQL.{fname}"

        appendable = fdata.get("appendable")

        # Arrays ──────────────────────────────────────────────────────────────
        if field_type == "array":
            if appendable is True:
                occ      = fdata["occurrence_count"]
                total_el = fdata.get("total_elements", 0)
                avg_size = (total_el / occ) if occ > 0 else 0
                fdata["avg_size"] = round(avg_size, 2)
                return ("Mongo", "Mongo.embed") if avg_size <= AVG_SIZE_THRESHOLD \
                       else ("Mongo", "Mongo.reference")
            # appendable=false (or None): arrays are inherently one-to-many
            # -> SQL child table (one_to_many is auto-set in phase1)
            return "SQL", f"SQL.{fname}"

        # Objects ─────────────────────────────────────────────────────────────
        # (independent_query already handled above)
        return "Mongo", "Mongo.embed"

    return "Buffer", "Buffer"


def phase4_classify(meta: dict, freq_map: dict):
    """
    Classify every top-level field and recursively propagate to all children.
    """
    global_key = meta["global_key"]
    fields     = meta["fields"]
    top_level  = {k: v for k, v in fields.items() if v["parent"] is None}

    print("\n" + "=" * 60)
    print("  PHASE 4  -  STORAGE CLASSIFICATION")
    print("=" * 60)

    for fname, fdata in top_level.items():
        freq = freq_map.get(fname, 0.0)
        backend, detail = _classify_entity(fname, fdata, freq, global_key)

        fdata["storage_backend"] = backend
        fdata["storage_detail"]  = detail
        _propagate(fname, backend, detail, fields)

        avg_info = (f"  avg_size={fdata.get('avg_size', '-')}"
                    if fdata["type"] == "array" else "")
        print(f"  {fname:<25}  freq={freq*100:5.1f}%  ->  {detail}{avg_info}")

    print("=" * 60)


# ---------------------------------------------------------------------------
# Phase 5  -  Key Management
# ---------------------------------------------------------------------------

def _depluralize(table_name: str) -> str:
    """
    Simple de-pluralisation to derive entity base name from table name.
    e.g.  orders -> order,  reviews -> review,  categories -> category
    """
    if table_name.endswith("ies"):
        return table_name[:-3] + "y"
    if table_name.endswith("ses") or table_name.endswith("xes") or table_name.endswith("ches"):
        return table_name[:-2]
    if table_name.endswith("s"):
        return table_name[:-1]
    return table_name


def _resolve_child_pk(table: str, fields: dict) -> tuple[str, bool]:
    """
    Determine the PRIMARY KEY for a child SQL table using surrogate key logic:

    Step 1 – Look for a field with unique=true inside this entity → natural PK.
    Step 2 – Look for a field named <entity_base>_id (e.g. order_id for orders)
             → treat it as natural PK even without the unique flag.
    Step 3 – No match → generate surrogate key <entity_base>_id (AUTOINCREMENT).

    Returns (pk_column_name, is_surrogate).
    """
    entity_base  = _depluralize(table)            # "orders" -> "order"
    candidate_pk = f"{entity_base}_id"            # "order_id"

    table_detail = f"SQL.{table}"
    table_fields = {
        k: v for k, v in fields.items()
        if v.get("storage_detail") == table_detail
        and v["type"] not in ("object", "array")
    }

    # Step 1: unique field (schema-declared or confirmed by data)
    for fname, fdata in table_fields.items():
        if fdata.get("unique"):
            return fname.split(".")[-1], False     # natural key

    # Step 2: field named <entity>_id exists in the entity
    for fname in table_fields:
        col = fname.split(".")[-1]
        if col == candidate_pk:
            return candidate_pk, False             # natural key (no unique flag needed)

    # Step 3: surrogate - system will AUTOINCREMENT this column
    return candidate_pk, True


def phase5_key_management(meta: dict) -> dict:
    """
    Derive PK/FK roles for every SQL table and key rules for Mongo/Buffer.

    Main table  : global_key is PRIMARY KEY.
    Child tables: surrogate key logic (see _resolve_child_pk).
                  global_key is FOREIGN KEY for linking.

    Entirely metadata-driven - no field names hardcoded.
    """
    global_key  = meta["global_key"]
    fields      = meta["fields"]
    main_table  = _main_table_name(global_key)

    sql_tables    = {}
    mongo_embeds  = []
    mongo_refs    = []
    buffer_fields = []

    # ── First pass: collect columns per table ──────────────────────────────
    for fname, fdata in fields.items():
        detail  = fdata.get("storage_detail") or "Buffer"
        backend = fdata.get("storage_backend") or "Buffer"

        if backend == "SQL":
            table = detail.split(".", 1)[1]
            if table not in sql_tables:
                sql_tables[table] = {
                    "columns":     [],
                    "primary_key": None,
                    "foreign_key": None,
                    "surrogate":   False,
                }

            entry = sql_tables[table]

            # Skip object/array containers
            if fdata["type"] in ("object", "array"):
                continue

            col = fname.split(".")[-1]
            if col not in entry["columns"]:
                entry["columns"].append(col)

        elif detail in ("Mongo.embed", "Mongo.document"):
            if fname not in mongo_embeds:
                mongo_embeds.append(fname)
        elif detail == "Mongo.reference":
            if fname not in mongo_refs:
                mongo_refs.append(fname)
        else:
            if fname not in buffer_fields:
                buffer_fields.append(fname)

    # ── Second pass: assign PK / FK per table ──────────────────────────────
    for table, entry in sql_tables.items():
        if table == main_table:
            # Main table: global_key is always PK
            entry["primary_key"] = global_key
            entry["surrogate"]   = False
        else:
            # Child table: surrogate key logic
            pk_col, is_surrogate = _resolve_child_pk(table, fields)
            entry["primary_key"] = pk_col
            entry["surrogate"]   = is_surrogate
            entry["foreign_key"] = global_key

            # Ensure FK column is present
            if global_key not in entry["columns"]:
                entry["columns"].insert(0, global_key)

            # If surrogate, PK column is generated - not from data
            # If natural, make sure PK column is listed first (after FK)
            if not is_surrogate and pk_col in entry["columns"]:
                entry["columns"].remove(pk_col)
                # Insert after FK
                fk_idx = entry["columns"].index(global_key) if global_key in entry["columns"] else -1
                entry["columns"].insert(fk_idx + 1, pk_col)

    # ── Third pass: build index specs per table ─────────────────────────────
    for table, entry in sql_tables.items():
        indexes   = []
        pk_col    = entry["primary_key"]
        fk_col    = entry.get("foreign_key")
        table_det = f"SQL.{table}"

        # Index on FK column so JOINs and WHERE on FK are O(log n)
        if fk_col:
            indexes.append({"column": fk_col, "unique": False})

        # UNIQUE index for every non-PK column declared unique in the schema
        for fname, fdata in fields.items():
            if fdata.get("storage_detail") != table_det:
                continue
            if fdata["type"] in ("object", "array"):
                continue
            col = fname.split(".")[-1]
            if col == pk_col:
                continue          # PK already carries implicit uniqueness
            if fdata.get("unique"):
                indexes.append({"column": col, "unique": True})

        entry["indexes"] = indexes

    key_map = {
        "SQL":    sql_tables,
        "Mongo":  {"embed": mongo_embeds, "reference": mongo_refs},
        "Buffer": buffer_fields,
    }

    print("\n" + "=" * 60)
    print("  PHASE 5  -  KEY MANAGEMENT")
    print("=" * 60)
    for table, info in sql_tables.items():
        pk_note = "(surrogate/AUTOINCREMENT)" if info["surrogate"] else "(natural)"
        print(f"  SQL.{table}")
        print(f"    PK      : {info['primary_key']}  {pk_note}")
        print(f"    FK      : {info['foreign_key']}")
        print(f"    columns : {info['columns']}")
        for idx in info.get("indexes", []):
            kind = "UNIQUE INDEX" if idx["unique"] else "INDEX"
            print(f"    {kind:<12}: {idx['column']}")
    print(f"\n  Mongo embed     : {mongo_embeds}")
    print(f"  Mongo reference : {mongo_refs}")
    print(f"  Buffer          : {buffer_fields}")
    print("=" * 60)

    return key_map


# ---------------------------------------------------------------------------
# Phase 6  -  Final Storage Mapping
# ---------------------------------------------------------------------------

def phase6_storage_map(meta: dict, key_map: dict):
    """
    Embed key_management into metadata_store.json directly.
    No separate storage_map.json is created.
    """
    # Write key_management into the metadata itself
    meta["key_management"] = key_map

    print("\n" + "=" * 60)
    print("  PHASE 6  -  FINAL STORAGE MAP")
    print("=" * 60)
    for fname, fdata in sorted(meta["fields"].items()):
        dest = fdata.get("storage_detail") or "Buffer"
        print(f"  {fname:<30}  ->  {dest}")
    print("=" * 60)
    print(f"\n  Metadata updated  in {METADATA_FILE}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    meta = _load(METADATA_FILE)

    if meta.get("total_records", 0) == 0:
        sys.exit("[ERROR] No records in metadata - run Phase 2 first.")

    freq_map = phase3_field_analysis(meta)
    phase4_classify(meta, freq_map)
    key_map  = phase5_key_management(meta)
    phase6_storage_map(meta, key_map)

    _save(METADATA_FILE, meta)
