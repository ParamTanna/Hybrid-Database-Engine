import json
import os
from typing import Any

from hybriddb.config import paths

AUDIT_FILE = paths.AUDIT_FILE


def _load_store() -> dict:
    if not os.path.exists(AUDIT_FILE):
        return {"global_key": None, "records": {}}
    try:
        with open(AUDIT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {"global_key": None, "records": {}}

    if not isinstance(data, dict):
        return {"global_key": None, "records": {}}
    data.setdefault("global_key", None)
    data.setdefault("records", {})
    if not isinstance(data["records"], dict):
        data["records"] = {}
    return data


def _save_store(store: dict):
    with open(AUDIT_FILE, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, ensure_ascii=True)


def upsert_created(global_key: str, key_value: Any, timestamp_utc: str):
    store = _load_store()
    store["global_key"] = global_key
    key = str(key_value)
    rec = store["records"].get(key, {})
    rec["created_at"] = rec.get("created_at") or timestamp_utc
    rec["last_updated_at"] = timestamp_utc
    store["records"][key] = rec
    _save_store(store)


def touch_updated(global_key: str, key_value: Any, timestamp_utc: str):
    store = _load_store()
    store["global_key"] = global_key
    key = str(key_value)
    rec = store["records"].get(key, {})
    if not rec.get("created_at"):
        rec["created_at"] = timestamp_utc
    rec["last_updated_at"] = timestamp_utc
    store["records"][key] = rec
    _save_store(store)


def get_entries(global_key: str, key_values: list[Any]) -> dict[str, dict]:
    store = _load_store()
    if store.get("global_key") not in (None, global_key):
        return {}
    records = store.get("records", {})
    out: dict[str, dict] = {}
    for v in key_values:
        key = str(v)
        row = records.get(key)
        if isinstance(row, dict):
            out[key] = row
    return out


def clear_store() -> bool:
    if os.path.exists(AUDIT_FILE):
        os.remove(AUDIT_FILE)
        return True
    return False
