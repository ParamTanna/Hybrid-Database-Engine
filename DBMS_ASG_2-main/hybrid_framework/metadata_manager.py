import json
import os
import shutil
from pathlib import Path
from typing import Any
import hybrid_framework.config as config


# ──────────────────────────────────────────────────────────────────────────────
# Core load / save
# ──────────────────────────────────────────────────────────────────────────────

def load() -> dict:
    """Load metadata.json; return {} if missing or corrupted."""
    if not config.METADATA_FILE.exists():
        return {}
    try:
        with open(config.METADATA_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save(data: dict) -> None:
    """Write metadata.json atomically (write to .tmp, then rename)."""
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    temp_file = config.METADATA_FILE.with_suffix(".tmp")
    try:
        with open(temp_file, "w") as f:
            json.dump(data, f, indent=2)
        shutil.move(str(temp_file), str(config.METADATA_FILE))
    except OSError:
        if temp_file.exists():
            os.remove(temp_file)


# ──────────────────────────────────────────────────────────────────────────────
# Schema
# ──────────────────────────────────────────────────────────────────────────────

def save_schema(schema_dict: dict) -> None:
    """Update the 'schema' key and save."""
    data = load()
    data["schema"] = schema_dict
    save(data)


def get_schema() -> dict:
    """Return metadata['schema'] or {}."""
    return load().get("schema", {})


# ──────────────────────────────────────────────────────────────────────────────
# Cumulative statistics
# ──────────────────────────────────────────────────────────────────────────────

def save_cumulative_stats(cumulative_raw: dict, total: int) -> None:
    data = load()
    data["cumulative_stats"]   = cumulative_raw
    data["total_records_seen"] = total
    save(data)


def get_cumulative_stats() -> dict:
    return load().get("cumulative_stats", {"total_records": 0, "fields": {}})


# ──────────────────────────────────────────────────────────────────────────────
# Field placement
# ──────────────────────────────────────────────────────────────────────────────

def save_field_placement(placement: dict) -> None:
    """Merge new placement decisions into existing 'field_placement' and save."""
    data = load()
    existing = data.get("field_placement", {})
    existing.update(placement)
    data["field_placement"] = existing
    save(data)


def get_field_placement() -> dict:
    return load().get("field_placement", {})


def get_placement_for_field(field_name: str) -> dict | None:
    return get_field_placement().get(field_name)


# ──────────────────────────────────────────────────────────────────────────────
# SQL tables
# ──────────────────────────────────────────────────────────────────────────────

def save_sql_tables(tables_info: dict) -> None:
    """Merge into 'sql_tables'; also persist flattened_objects if present.

    tables_info may be the full result dict from build_sql_table_schema:
        {"tables": {...}, "flattened_objects": {...}, "dimension_tables_meta": {...}}
    or just a plain {table_name: table_info} dict.

    Everything is written in a single save() call so the two keys cannot
    overwrite each other (the previous implementation called save_flattened_objects()
    then save(data) where data was loaded before the flattened_objects write,
    silently discarding what save_flattened_objects had just persisted).
    """
    data = load()
    existing = data.get("sql_tables", {})
    if "tables" in tables_info:
        existing.update(tables_info["tables"])
    else:
        existing.update(tables_info)
    data["sql_tables"] = existing
    if "flattened_objects" in tables_info:
        flat_existing = data.get("flattened_objects", {})
        flat_existing.update(tables_info["flattened_objects"])
        data["flattened_objects"] = flat_existing
    save(data)


def get_sql_tables() -> dict:
    return load().get("sql_tables", {})


# ──────────────────────────────────────────────────────────────────────────────
# MongoDB collections
# ──────────────────────────────────────────────────────────────────────────────

def save_mongo_collections(collections: dict) -> None:
    data = load()
    existing = data.get("mongo_collections", {})
    existing.update(collections)
    data["mongo_collections"] = existing
    save(data)


def get_mongo_collections() -> dict:
    return load().get("mongo_collections", {})


# ──────────────────────────────────────────────────────────────────────────────
# Flattened objects  (inline nested-object columns)
# ──────────────────────────────────────────────────────────────────────────────

def save_flattened_objects(flattened: dict) -> None:
    data = load()
    existing = data.get("flattened_objects", {})
    existing.update(flattened)
    data["flattened_objects"] = existing
    save(data)


def get_flattened_objects() -> dict:
    """Returns mapping of original field name → list of dot-notation column names."""
    return load().get("flattened_objects", {})


# ──────────────────────────────────────────────────────────────────────────────
# 3NF dimension tables  (NEW)
# ──────────────────────────────────────────────────────────────────────────────

def save_3nf_dimension_tables(dim_tables: dict) -> None:
    """
    Persist the 3NF dimension-table metadata produced by normalization_engine.

    dim_tables has the shape:
        {
            "student_id_dim": {
                "determinant":      "student_id",
                "dependent_fields": ["username", "name", "email"],
                "all_fields":       ["student_id", "username", "name", "email"],
            },
            ...
        }

    This metadata is consumed by CRUDManager to:
      • Route INSERT operations: dimension rows go into the dim table first
        (INSERT OR IGNORE so repeated FK values don't error), then the main
        'records' row is written without the dependent columns.
      • Route READ  operations: JOINed on the FK (determinant) column to
        reconstruct full records from the normalised tables.
      • Route UPDATE operations: dependent fields are updated in their dim table.
    """
    data = load()
    existing = data.get("3nf_dimension_tables", {})
    existing.update(dim_tables)
    data["3nf_dimension_tables"] = existing
    save(data)


def get_3nf_dimension_tables() -> dict:
    """
    Return the stored 3NF dimension-table metadata, or {} if none has been saved.

    Shape of each entry:
        {
            "determinant":      str,    # PK of the dimension table / FK in records
            "dependent_fields": list,   # non-PK fields housed in the dim table
            "all_fields":       list,   # every column in the dim table
        }
    """
    return load().get("3nf_dimension_tables", {})


# ──────────────────────────────────────────────────────────────────────────────
# Record counts
# ──────────────────────────────────────────────────────────────────────────────

def increment_total_records(n: int) -> None:
    data = load()
    data["total_records_seen"] = data.get("total_records_seen", 0) + n
    save(data)


def get_total_records() -> int:
    return load().get("total_records_seen", 0)


# ──────────────────────────────────────────────────────────────────────────────
# Reset
# ──────────────────────────────────────────────────────────────────────────────

def reset() -> None:
    """Delete metadata.json and buffer.json, clearing all persisted state."""
    if config.METADATA_FILE.exists():
        os.remove(config.METADATA_FILE)
    if config.BUFFER_FILE.exists():
        os.remove(config.BUFFER_FILE)
