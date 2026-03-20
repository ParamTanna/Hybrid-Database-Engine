from datetime import datetime, timezone
from typing import Any
import hybrid_framework.config as config

def _try_numeric(s: str) -> int | float | str:
    """If string is convertible to int or float, return that; else return original string."""
    if not isinstance(s, str):
        return s
    try:
        if "." in s:
            return float(s)
        return int(s)
    except ValueError:
        return s

def coerce_numeric_strings(record: dict[str, Any]) -> dict[str, Any]:
    """Convert string values to int/float when possible (top-level only)."""
    coerced = {}
    for k, v in record.items():
        coerced[k] = _try_numeric(v)
    return coerced

def ingest_one(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Coerce numeric strings and then add sys_ingested_at.
    Assumes incoming keys match schema exactly.
    """
    # 1. Coerce types
    record = coerce_numeric_strings(raw)
    
    # 2. Add system timestamp
    record[config.JOIN_KEY] = datetime.now(timezone.utc).isoformat()
    
    return record
