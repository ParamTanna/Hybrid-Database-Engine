from typing import Any
import hybrid_framework.metadata_manager as metadata_manager

VALID_TYPES = {"int", "float", "str", "bool", "array", "object"}

def validate_schema(schema_dict: dict) -> tuple[bool, str]:
    """Returns (True, "") if valid, (False, error_message) if not."""
    if "fields" not in schema_dict:
        return False, "Schema must have 'fields' key."
    
    fields = schema_dict["fields"]
    if not isinstance(fields, dict):
        return False, "'fields' must be a dictionary."
        
    for field_name, props in fields.items():
        if not isinstance(props, dict):
            return False, f"Properties for field '{field_name}' must be a dictionary."
        if "type" not in props:
            return False, f"Field '{field_name}' is missing 'type'."
        if props["type"] not in VALID_TYPES:
            return False, f"Field '{field_name}' has invalid type '{props['type']}'. Valid: {VALID_TYPES}"
        
        # Validate constraints
        for constraint in ["not_null", "unique"]:
            if constraint in props and not isinstance(props[constraint], bool):
                return False, f"Constraint '{constraint}' for field '{field_name}' must be a boolean."
                
    return True, ""

def register_schema(schema_dict: dict) -> dict:
    """Registers the schema and returns a summary."""
    is_valid, error = validate_schema(schema_dict)
    if not is_valid:
        raise ValueError(error)
        
    metadata_manager.save_schema(schema_dict)
    
    fields = schema_dict["fields"]
    summary = {
        "fields_registered": len(fields),
        "array_fields": [f for f, p in fields.items() if p["type"] == "array"],
        "object_fields": [f for f, p in fields.items() if p["type"] == "object"],
        "scalar_fields": [f for f, p in fields.items() if p["type"] not in ("array", "object")]
    }
    return summary

def get_schema() -> dict:
    """Returns the registered schema from metadata."""
    return metadata_manager.get_schema()

def schema_field_names() -> set[str]:
    """Returns the set of all field names declared in the schema."""
    schema = get_schema()
    if not schema:
        return set()
    return set(schema.get("fields", {}).keys())
