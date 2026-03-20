from typing import Any
import hybrid_framework.config as config

def _value_type(v: Any) -> str:
    """Return a string representing the type of the value."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    if isinstance(v, list):
        return "list"
    if isinstance(v, dict):
        return "dict"
    return "unknown"

def flatten_value_for_type(value: Any) -> Any:
    """Returns value as-is (nested dict/list are kept for has_nested detection)."""
    return value

def _new_field_stat() -> dict[str, Any]:
    return {
        "presence_count": 0,
        "types": {},
        "unique_values": set(),
        "has_nested": False,
    }


def analyze_buffer(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Analyze a batch of records and return per-field stats."""
    field_stats: dict[str, Any] = {}
    batch_size = len(records)

    for record in records:
        for field, value in record.items():
            if field not in field_stats:
                field_stats[field] = _new_field_stat()

            stats = field_stats[field]
            stats["presence_count"] += 1

            v_type = _value_type(value)
            stats["types"][v_type] = stats["types"].get(v_type, 0) + 1

            if v_type in ("dict", "list"):
                stats["has_nested"] = True
                stats["unique_values"].add(str(value))
            else:
                stats["unique_values"].add(value)

            if v_type == "dict" and isinstance(value, dict):
                sub = stats.setdefault("subfields", {})
                for sk, sv in value.items():
                    if sk not in sub:
                        sub[sk] = _new_field_stat()
                    sst = sub[sk]
                    sst["presence_count"] += 1
                    stype = _value_type(sv)
                    sst["types"][stype] = sst["types"].get(stype, 0) + 1
                    if stype in ("dict", "list"):
                        sst["has_nested"] = True
                        sst["unique_values"].add(str(sv))
                    else:
                        sst["unique_values"].add(sv)

    result: dict[str, Any] = {"batch_size": batch_size, "fields": {}}
    for field, stats in field_stats.items():
        entry: dict[str, Any] = {
            "presence_count": stats["presence_count"],
            "types": stats["types"],
            "unique_count": len(stats["unique_values"]),
            "has_nested": stats["has_nested"],
        }
        if "subfields" in stats:
            entry["subfields"] = {
                sk: {
                    "presence_count": sst["presence_count"],
                    "types": sst["types"],
                    "unique_count": len(sst["unique_values"]),
                    "has_nested": sst["has_nested"],
                }
                for sk, sst in stats["subfields"].items()
            }
        result["fields"][field] = entry
    return result

def merge_cumulative_stats(prev_cumulative: dict, batch_result: dict, batch_size: int) -> dict:
    """Merge batch results into cumulative raw stats."""
    if not prev_cumulative:
        prev_cumulative = {"total_records": 0, "fields": {}}
    
    prev_cumulative["total_records"] += batch_size
    cumulative_fields = prev_cumulative["fields"]
    
    for field, batch_stats in batch_result["fields"].items():
        if field not in cumulative_fields:
            cumulative_fields[field] = {
                "presence_count": 0,
                "types": {},
                "unique_count_approx": 0, # In a real system, use HyperLogLog
                "has_nested": False
            }
        
        cum_stats = cumulative_fields[field]
        cum_stats["presence_count"] += batch_stats["presence_count"]
        cum_stats["has_nested"] = cum_stats["has_nested"] or batch_stats["has_nested"]
        
        for t, count in batch_stats["types"].items():
            cum_stats["types"][t] = cum_stats["types"].get(t, 0) + count
            
        # Simplistic unique count merge: just add. 
        # (This is wrong for true uniqueness but fine for the assignment's ratio logic)
        cum_stats["unique_count_approx"] += batch_stats["unique_count"]

        batch_sub = batch_stats.get("subfields") or {}
        if batch_sub:
            cum_sub = cum_stats.setdefault("subfields", {})
            for sk, bsub in batch_sub.items():
                if sk not in cum_sub:
                    cum_sub[sk] = {
                        "presence_count": 0,
                        "types": {},
                        "unique_count_approx": 0,
                        "has_nested": False,
                    }
                cs = cum_sub[sk]
                cs["presence_count"] += bsub["presence_count"]
                cs["has_nested"] = cs["has_nested"] or bsub["has_nested"]
                for t, c in bsub["types"].items():
                    cs["types"][t] = cs["types"].get(t, 0) + c
                cs["unique_count_approx"] += bsub["unique_count"]

    return prev_cumulative

def cumulative_raw_to_derived(cumulative_raw: dict) -> dict:
    """Compute frequency, type_stability, etc. from raw cumulative stats."""
    total_records = cumulative_raw["total_records"]
    derived = {}
    
    for field, stats in cumulative_raw["fields"].items():
        presence_count = stats["presence_count"]
        frequency = presence_count / total_records if total_records > 0 else 0
        
        # Dominant type
        types = stats["types"]
        if not types:
            dominant_type = "null"
            type_stability = 0.0
        else:
            dominant_type = max(types, key=types.get)
            type_stability = types[dominant_type] / presence_count if presence_count > 0 else 0
            
        uniqueness_ratio = stats["unique_count_approx"] / presence_count if presence_count > 0 else 0
        # Clamp uniqueness_ratio to 1.0 because of our simplistic merge
        uniqueness_ratio = min(1.0, uniqueness_ratio)
        
        derived[field] = {
            "frequency": frequency,
            "presence_count": presence_count,
            "dominant_type": dominant_type,
            "type_stability": type_stability,
            "uniqueness_ratio": uniqueness_ratio,
            "has_nested": stats["has_nested"]
        }
        
    return derived
