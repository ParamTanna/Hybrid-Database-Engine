"""
Phase 1 - Schema Registration
==============================
Reads schema.json, parses every field recursively, builds the metadata
registry and saves it to metadata_store.json.

Run:
    python phase1_schema_registration.py
"""

from typing import Any
import json
import os
import sys
from datetime import datetime, timezone

METADATA_FILE = "metadata_store.json"
SCHEMA_FILE   = "schema.json"


# ---------------------------------------------------------------------------
# Metadata builder
# ---------------------------------------------------------------------------

def _empty_field_entry(
    field_name: str,
    field_type: str,
    level: int,
    parent: str | None,
    not_null: bool = False,
    unique: bool = False,
    appendable: bool | None = None,
    independent_query: bool | None = None,
) -> dict:
    # one_to_many is auto-deduced: every array field is inherently one-to-many.
    # Users no longer need to declare it in schema.json.
    one_to_many = True if field_type == "array" else None

    return {
        "field_name":        field_name,
        "type":              field_type,
        "level":             level,
        # constraints — start from schema; Phase 2 overwrites from observed data
        "not_null":          not_null,
        "unique":            unique,
        "appendable":        appendable,
        "independent_query": independent_query,
        "one_to_many":       one_to_many,   # auto-set, not from schema
        "parent":            parent,
        "children":          [],
        # stat counters — Phase 2 increments
        "occurrence_count":  0,
        "total_elements":    0,
        # classification — Phase 4 fills
        "storage_backend":   None,
        "storage_detail":    None,
    }


def _parse_fields(
    fields: dict[str, Any],
    registry: dict,
    level: int,
    parent: str | None,
) -> list[str]:
    """Recursively walk a fields block and populate registry."""
    children_keys = []
    for field_name, field_def in fields.items():
        qualified = f"{parent}.{field_name}" if parent else field_name

        entry = _empty_field_entry(
            field_name        = qualified,
            field_type        = field_def.get("type", "unknown"),
            level             = level,
            parent            = parent,
            not_null          = field_def.get("not_null", False),
            unique            = field_def.get("unique", False),
            appendable        = field_def.get("appendable"),
            independent_query = field_def.get("independent_query"),
            # one_to_many is intentionally NOT read from schema — auto-deduced
        )

        registry[qualified] = entry
        children_keys.append(qualified)

        if "fields" in field_def and isinstance(field_def["fields"], dict):
            entry["children"] = _parse_fields(
                fields   = field_def["fields"],
                registry = registry,
                level    = level + 1,
                parent   = qualified,
            )

    return children_keys


def build_metadata(raw_schema: dict[str, Any]) -> dict:
    global_key = raw_schema.get("global_key")
    if not global_key:
        raise ValueError("Schema must contain a 'global_key' field.")

    registry: dict[str, Any] = {}
    field_defs = {
        k: v for k, v in raw_schema.items()
        if k != "global_key" and isinstance(v, dict)
    }
    _parse_fields(fields=field_defs, registry=registry, level=0, parent=None)

    return {
        "global_key":    global_key,
        "registered_at": datetime.now(timezone.utc).isoformat(),
        "total_records": 0,
        "fields":        registry,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not os.path.exists(SCHEMA_FILE):
        sys.exit(f"[ERROR] {SCHEMA_FILE} not found.")

    with open(SCHEMA_FILE, "r") as f:
        raw_schema = json.load(f)

    metadata = build_metadata(raw_schema)

    with open(METADATA_FILE, "w") as f:
        json.dump(metadata, f, indent=2)

    print("\n" + "=" * 55)
    print("  PHASE 1 - SCHEMA REGISTRATION COMPLETE")
    print("=" * 55)
    print(f"  global_key  : {metadata['global_key']}")
    print(f"  registered  : {metadata['registered_at']}")
    print(f"  fields      : {len(metadata['fields'])} registered")
    print("-" * 55)
    for fname, fdata in metadata["fields"].items():
        indent = "    " * fdata["level"]
        flags = []
        if fdata["appendable"]        is not None: flags.append(f"appendable={fdata['appendable']}")
        if fdata["independent_query"] is not None: flags.append(f"iq={fdata['independent_query']}")
        if fdata["one_to_many"]       is not None: flags.append(f"1tm={fdata['one_to_many']}")
        flag_str = f"  [{', '.join(flags)}]" if flags else ""
        print(f"  {indent}{fname}  ({fdata['type']}){flag_str}")
    print("=" * 55)
    print(f"\n  Metadata saved to {METADATA_FILE}\n")
