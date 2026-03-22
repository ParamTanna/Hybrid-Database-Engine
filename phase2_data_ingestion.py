"""
Phase 2 - Data Ingestion
=========================
ALL incoming data -> BUFFER first -> Phase 3 analyses -> Phase 4 routes

Step 1 - Validation
  Check each field type against schema. Allow safe conversions
  (string->int/float). Discard only the mismatched field, not the whole record.

Step 2 - New Field Handling
  Top-level unknown  -> register in metadata (unknown_type, low_confidence),
                        tag for BUFFER
  Nested unknown     -> inherit parent storage, register in metadata,
                        do NOT buffer

Step 3 - Metadata Stat Update
  total_records      += 1  (global)
  occurrence_count   += 1  (per field seen)
  total_elements     += len(array)  (array fields only)

Step 4 - Constraint Update (data wins over schema declaration)
  not_null  <- True only if field appeared in every single record
  unique    <- True only if no duplicate values were seen across all records

Outputs:
  buffer.json         - every ingested record (fields spread flat + metadata tags)
  metadata_store.json - updated occurrence counts

Run:
    python phase2_data_ingestion.py
"""

import json
import os
import sys
import requests
from datetime import datetime, timezone

METADATA_FILE  = "metadata_store.json"
STREAM_BASE    = "http://127.0.0.1:8000/record"
DEFAULT_COUNT  = 100

from buffer_store import (
    staging_load   as load_buffer,
    staging_save   as save_buffer,
    BUFFER_FILE,
)


# ---------------------------------------------------------------------------
# Type validation
# ---------------------------------------------------------------------------

def _validate_field(value, expected_type: str):
    """Returns (coerced_value, ok). Attempts safe conversion for int/float."""
    if expected_type == "string":
        return str(value), True

    if expected_type == "boolean":
        return (value, True) if isinstance(value, bool) else (None, False)

    if expected_type == "int":
        if isinstance(value, bool):  return None, False
        if isinstance(value, int):   return value, True
        try:    return int(value), True
        except: return None, False

    if expected_type == "float":
        if isinstance(value, bool):          return None, False
        if isinstance(value, (int, float)):  return float(value), True
        try:    return float(value), True
        except: return None, False

    if expected_type == "array":
        return (value, True) if isinstance(value, list) else (None, False)

    if expected_type == "object":
        return (value, True) if isinstance(value, dict) else (None, False)

    return value, True


def _infer_type(value) -> str:
    if isinstance(value, bool):  return "boolean"
    if isinstance(value, int):   return "int"
    if isinstance(value, float): return "float"
    if isinstance(value, list):  return "array"
    if isinstance(value, dict):  return "object"
    return "string"


# ---------------------------------------------------------------------------
# Metadata helpers
# ---------------------------------------------------------------------------

def _load_metadata() -> dict:
    if not os.path.exists(METADATA_FILE):
        sys.exit(f"[ERROR] {METADATA_FILE} not found - run Phase 1 first.")
    with open(METADATA_FILE, "r") as f:
        return json.load(f)


def _save_metadata(meta: dict):
    with open(METADATA_FILE, "w") as f:
        json.dump(meta, f, indent=2)


def _ensure_field_entry(meta: dict, qualified: str, value, parent: str | None):
    """Register an unknown field in metadata if not already present."""
    if qualified not in meta["fields"]:
        parent_level = (meta["fields"][parent]["level"]
                        if parent and parent in meta["fields"] else -1)
        meta["fields"][qualified] = {
            "field_name":        qualified,
            "type":              _infer_type(value),
            "level":             parent_level + 1,
            "not_null":          False,
            "unique":            False,
            "appendable":        None,
            "independent_query": None,
            "one_to_many":       None,
            "parent":            parent,
            "children":          [],
            "confidence":        "low",
            "occurrence_count":  0,
            "total_elements":    0,
            "storage_backend":   None,
            "storage_detail":    None,
        }
        if parent and parent in meta["fields"]:
            if qualified not in meta["fields"][parent]["children"]:
                meta["fields"][parent]["children"].append(qualified)


# ---------------------------------------------------------------------------
# Single-record processing
# ---------------------------------------------------------------------------

def process_record(record: dict, meta: dict, seen_values: dict) -> dict:
    """
    Validate fields, handle unknowns, update metadata stats, track distinct values.

    seen_values : dict[field_name -> set]
        Accumulated across the whole batch; updated in-place.
        Used by _update_constraints_from_data() after all records are processed.

    Returns a buffer entry with fields spread flat.
    """
    schema_fields = meta["fields"]

    validated   = {}
    unknown_top = {}
    discarded   = {}

    for key, value in record.items():

        if key in schema_fields:
            fmeta    = schema_fields[key]
            expected = fmeta["type"]

            if expected in ("array", "object"):
                validated[key] = value

                if expected == "array" and isinstance(value, list):
                    fmeta["total_elements"] += len(value)
                    for item in value:
                        if isinstance(item, dict):
                            for sub_key, sub_val in item.items():
                                qname = f"{key}.{sub_key}"
                                _ensure_field_entry(meta, qname, sub_val, parent=key)
                                meta["fields"][qname]["occurrence_count"] += 1
                                _track_value(seen_values, qname, sub_val)

                elif expected == "object" and isinstance(value, dict):
                    for sub_key, sub_val in value.items():
                        qname = f"{key}.{sub_key}"
                        _ensure_field_entry(meta, qname, sub_val, parent=key)
                        meta["fields"][qname]["occurrence_count"] += 1
                        _track_value(seen_values, qname, sub_val)

                fmeta["occurrence_count"] += 1

            else:
                coerced, ok = _validate_field(value, expected)
                if ok:
                    validated[key] = coerced
                    fmeta["occurrence_count"] += 1
                    _track_value(seen_values, key, coerced)
                else:
                    discarded[key] = {"value": value, "expected": expected}

        else:
            # Top-level unknown -> BUFFER
            unknown_top[key] = value
            _ensure_field_entry(meta, key, value, parent=None)
            meta["fields"][key]["occurrence_count"] += 1
            _track_value(seen_values, key, value)

    meta["total_records"] += 1

    return {
        **validated,
        "unknown_top": unknown_top,
        "discarded":   discarded,
        "received_at": datetime.now(timezone.utc).isoformat(),
    }


def _track_value(seen_values: dict, field: str, value):
    """Add a value to the per-field distinct-value tracker (skip unhashable types)."""
    try:
        if field not in seen_values:
            seen_values[field] = set()
        seen_values[field].add(value)
    except TypeError:
        pass


def _update_constraints_from_data(meta: dict, seen_values: dict):
    """
    Called once after all records in the batch are processed.
    Overwrites not_null and unique on every field using observed data.

    not_null <- True only when the field appeared in every single record.
    unique   <- True only when no duplicate values were seen across all records.
    """
    total = meta["total_records"]
    if total == 0:
        return

    for fname, fmeta in meta["fields"].items():
        occ = fmeta.get("occurrence_count", 0)

        # overwrite not_null: data must prove it was present every time
        fmeta["not_null"] = (occ == total)

        # overwrite unique: data must prove no value appeared twice
        seen = seen_values.get(fname)
        if seen is not None:
            fmeta["unique"] = (len(seen) == occ) and (occ > 0)
        else:
            # array/object containers — uniqueness not applicable
            fmeta["unique"] = False


# ---------------------------------------------------------------------------
# Buffer helpers  (delegate to buffer_store — MongoDB-backed)
# ---------------------------------------------------------------------------

def _load_buffer() -> dict:
    return load_buffer()


def _save_buffer(buf: dict):
    save_buffer(buf)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    raw = input(f"How many records to ingest? [default {DEFAULT_COUNT}]: ").strip()
    try:
        target = int(raw) if raw else DEFAULT_COUNT
        if target <= 0: raise ValueError
    except ValueError:
        print(f"Invalid input - using default {DEFAULT_COUNT}.")
        target = DEFAULT_COUNT

    url = f"{STREAM_BASE}/{target}"
    print(f"\n[Phase 2] Ingesting {target} records from {url} ...\n")

    meta        = _load_metadata()
    buf         = _load_buffer()       # loads from buffer.json (staging)
    seen_values: dict = {}   # field -> set of distinct values, built during this batch

    ingested    = 0
    n_validated = 0
    n_unknown   = 0
    n_discarded = 0

    try:
        with requests.get(url, stream=True, timeout=60) as resp:
            resp.raise_for_status()

            for raw_line in resp.iter_lines():
                if ingested >= target:
                    break
                if not raw_line:
                    continue

                line = raw_line.decode("utf-8") if isinstance(raw_line, bytes) else raw_line
                if not line.startswith("data:"):
                    continue

                try:
                    record = json.loads(line[5:].strip())
                except json.JSONDecodeError:
                    continue

                entry = process_record(record, meta, seen_values)
                buf["records"].append(entry)
                buf["total_buffered"] += 1

                ingested    += 1
                n_fields     = len(entry) - 3   # exclude unknown_top, discarded, received_at
                n_unknown   += len(entry["unknown_top"])
                n_discarded += len(entry["discarded"])
                n_validated += n_fields

                print(f"  [{ingested:>4}/{target}]  "
                      f"customer_id={entry.get(meta['global_key'])}  "
                      f"fields={n_fields}  "
                      f"unknown={len(entry['unknown_top'])}  "
                      f"discarded={len(entry['discarded'])}")

    except requests.exceptions.ConnectionError:
        print("[ERROR] Cannot reach simulation server at http://127.0.0.1:8000")
        print("        Start it first:  python simulation_code_2.py")
        sys.exit(1)

    # ---------------------------------------------------------------
    # Overwrite not_null / unique on every field from observed data
    # ---------------------------------------------------------------
    _update_constraints_from_data(meta, seen_values)

    _save_buffer(buf)
    _save_metadata(meta)

    print("\n" + "=" * 60)
    print("  PHASE 2 - INGESTION COMPLETE")
    print("=" * 60)
    print(f"  Records ingested    : {ingested}")
    print(f"  Total records seen  : {meta['total_records']}")
    print(f"  Fields validated    : {n_validated}")
    print(f"  Unknown (buffered)  : {n_unknown}")
    print(f"  Discarded (mismatch): {n_discarded}")

    print("\n" + "-" * 60)
    print("  CONSTRAINTS (updated from data)")
    print(f"  {'Field':<30} {'not_null':>8} {'unique':>7}  {'occurrence':>10}")
    print("  " + "-" * 58)
    known = {k: v for k, v in meta["fields"].items()
             if v.get("confidence") != "low"}
    for fname, fdata in sorted(known.items(), key=lambda x: x[0]):
        nn   = "yes" if fdata.get("not_null") else "no"
        uq   = "yes" if fdata.get("unique")   else "no"
        occ  = fdata.get("occurrence_count", 0)
        freq = occ / meta["total_records"] * 100 if meta["total_records"] else 0
        print(f"  {fname:<30} {nn:>8} {uq:>7}  {occ:>5}/{meta['total_records']} ({freq:.0f}%)")

    print("=" * 60)
    print(f"\n  Staging buffer    -> {BUFFER_FILE}  (total={buf['total_buffered']})")
    print(f"  (Run DB Init to route fields to SQL/Mongo and flush buffer to MongoDB)")
    print(f"  Metadata saved to {METADATA_FILE}\n")
