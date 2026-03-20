from typing import Any
import hybrid_framework.config as config
import hybrid_framework.schema_registry as schema_registry

def classify_fields(field_stats: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """
    Classifies fields into SQL, MongoDB, or undecided.
    Returns for each field: {"backend": "sql"|"mongo"|"undecided", "unique": bool, "reason": str}
    """
    decisions = {}
    schema = schema_registry.get_schema()
    schema_fields = schema.get("fields", {})
    
    for field, stats in field_stats.items():
        # JOIN_KEY is always SQL
        if field == config.JOIN_KEY:
            decisions[field] = {
                "backend": "sql",
                "unique": True,
                "reason": "join_key"
            }
            continue

        # Insufficient observations check, but bypass if it's in the schema and required
        is_in_schema = field in schema_fields
        is_required = is_in_schema and schema_fields[field].get("not_null", False)
        
        if stats.get("presence_count", 0) < config.MIN_FIELD_OBSERVATIONS:
            if is_required:
                # Classify immediately based on schema type to avoid IntegrityErrors
                s_type = schema_fields[field]["type"]
                backend = "sql" if s_type not in ("array", "object") else "mongo"
                decisions[field] = {
                    "backend": backend,
                    "unique": schema_fields[field].get("unique", False),
                    "reason": "schema_required_immediate"
                }
            else:
                decisions[field] = {
                    "backend": "undecided",
                    "unique": False,
                    "reason": "insufficient_observations"
                }
            continue

        # Normalization logic
        frequency = stats.get("frequency", 0)
        type_stability = stats.get("type_stability", 0)
        has_nested = stats.get("has_nested", False)
        uniqueness_ratio = stats.get("uniqueness_ratio", 0)
        
        is_unique = uniqueness_ratio >= 0.95 or (is_in_schema and schema_fields[field].get("unique", False))
        
        if has_nested:
            decisions[field] = {
                "backend": "mongo",
                "unique": False,
                "reason": "nested_or_complex"
            }
        elif frequency >= config.FREQUENCY_THRESHOLD_SQL and type_stability >= config.TYPE_STABILITY_THRESHOLD:
            decisions[field] = {
                "backend": "sql",
                "unique": is_unique,
                "reason": "stable_flat"
            }
        else:
            decisions[field] = {
                "backend": "mongo",
                "unique": is_unique,
                "reason": "unstable_or_sparse"
            }
            
    return decisions
