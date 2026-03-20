from typing import Any
import hybrid_framework.config as config
import hybrid_framework.metadata_manager as metadata_manager

def _avg_array_length(field_name: str, sample_records: list[dict]) -> float:
    lengths = []
    for r in sample_records:
        if field_name in r and isinstance(r[field_name], list):
            lengths.append(len(r[field_name]))
    return sum(lengths) / len(lengths) if lengths else 0.0

def _items_contain_nested_arrays(field_name: str, sample_records: list[dict]) -> bool:
    for r in sample_records:
        if field_name in r and isinstance(r[field_name], list):
            for item in r[field_name]:
                if isinstance(item, list):
                    return True
                if isinstance(item, dict):
                    for val in item.values():
                        if isinstance(val, list):
                            return True
    return False

def _is_shared_structure(field_name: str, schema: dict, field_stats: dict) -> bool:
    # Heuristic: overlap of sub-fields
    return False # Simplified for now

def _is_update_heavy(field_name: str, sample_records: list[dict]) -> bool:
    sk = config.SECONDARY_JOIN_KEY
    if not sk:
        return False
    user_vals = {}  # secondary key -> list of values
    for r in sample_records:
        uname = r.get(sk)
        if not uname or field_name not in r:
            continue
        if uname not in user_vals: user_vals[uname] = []
        user_vals[uname].append(r[field_name])
    
    if not user_vals: return False
    
    changes = 0
    total_users = 0
    for vals in user_vals.values():
        if len(vals) < 2: continue
        total_users += 1
        if any(vals[i] != vals[0] for i in range(1, len(vals))):
            changes += 1
            
    return (changes / total_users) > 0.3 if total_users > 0 else False

def decide_strategy(field_name: str, field_stats: dict, schema: dict, sample_records: list[dict]) -> dict:
    stats = field_stats.get(field_name, {})
    d_type = stats.get("dominant_type")
    
    if d_type != "list" and d_type != "dict":
        return {"strategy": "embed", "reason": "scalar_field"}
        
    if d_type == "list":
        # Check if scalar array
        is_scalar_list = True
        for r in sample_records:
            if field_name in r and isinstance(r[field_name], list):
                for item in r[field_name]:
                    if isinstance(item, (dict, list)):
                        is_scalar_list = False
                        break
            if not is_scalar_list: break
            
        if is_scalar_list:
            return {"strategy": "embed", "reason": "scalar_array"}
            
        if _avg_array_length(field_name, sample_records) > config.MONGO_EMBED_MAX_ARRAY_LENGTH:
            return {"strategy": "reference", "reason": "large_array"}
            
        if _items_contain_nested_arrays(field_name, sample_records):
            return {"strategy": "reference", "reason": "nested_arrays_in_items"}
            
        if _is_update_heavy(field_name, sample_records):
            return {"strategy": "reference", "reason": "update_heavy"}

    return {"strategy": "embed", "reason": "small_stable_array_or_doc"}

def _mongo_anchor_field_names() -> list[str]:
    sk = config.SECONDARY_JOIN_KEY
    if sk:
        return [sk, config.JOIN_KEY]
    return [config.JOIN_KEY]


def build_mongo_collection_schema(mongo_fields: list[str], field_stats: dict, schema: dict, sample_records: list[dict]) -> dict:
    anchors = _mongo_anchor_field_names()
    collections = {
        "main_documents": {
            "strategy": "embed",
            "embedded_fields": [],
            "fields": list(anchors),
        }
    }

    for field in mongo_fields:
        if field == config.JOIN_KEY:
            continue
        if config.SECONDARY_JOIN_KEY and field == config.SECONDARY_JOIN_KEY:
            continue
            
        decision = decide_strategy(field, field_stats, schema, sample_records)
        if decision["strategy"] == "embed":
            collections["main_documents"]["embedded_fields"].append(field)
            collections["main_documents"]["fields"].append(field)
        else:
            # Reference
            sub_fields = set()
            for r in sample_records:
                if field in r and isinstance(r[field], list):
                    for item in r[field]:
                        if isinstance(item, dict):
                            sub_fields.update(item.keys())
            
            ref_fields = list(sub_fields) + list(anchors)
            collections[field] = {
                "strategy": "reference",
                "join_key": config.JOIN_KEY,
                "fields": ref_fields,
                "reason": decision["reason"]
            }
            
    return collections

def run_mongo_strategy(mongo_fields: list[str], field_stats: dict, schema: dict, sample_records: list[dict]) -> dict:
    result = build_mongo_collection_schema(mongo_fields, field_stats, schema, sample_records)
    metadata_manager.save_mongo_collections(result)
    return result
