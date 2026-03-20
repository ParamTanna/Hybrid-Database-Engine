import uuid
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
    Coerce numeric strings, then set the global correlation key (JOIN_KEY).

    Policy (from schema global_record_key, applied via config):
      - legacy_timestamp: JOIN_KEY = UTC ISO timestamp (default when schema omits global_record_key)
      - uuid_v4: JOIN_KEY = new UUID string
      - from_payload: JOIN_KEY must already be present on the record
    """
    record = coerce_numeric_strings(raw)
    policy = getattr(config, "GLOBAL_RECORD_KEY_POLICY", "legacy_timestamp")

    if policy == "legacy_timestamp":
        record[config.JOIN_KEY] = datetime.now(timezone.utc).isoformat()
    elif policy == "uuid_v4":
        record[config.JOIN_KEY] = str(uuid.uuid4())
    elif policy == "from_payload":
        jk = config.JOIN_KEY
        if jk not in record or record[jk] in (None, ""):
            raise ValueError(
                f"Record must include non-empty '{jk}' when global_record_key.policy is 'from_payload'."
            )
    else:
        record[config.JOIN_KEY] = datetime.now(timezone.utc).isoformat()

    return record
