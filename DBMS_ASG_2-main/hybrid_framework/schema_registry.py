from typing import Any
import hybrid_framework.config as config
import hybrid_framework.metadata_manager as metadata_manager

VALID_TYPES = {"int", "float", "str", "bool", "array", "object"}

SCALAR_TYPES = frozenset({"int", "float", "str", "bool"})


def build_nested_field_index(fields: dict) -> dict[str, dict[str, Any]]:
    """
    Flatten nested schema (object.properties, array items) into dot-path entries
    so metadata.json exposes 2nd+ level attributes without digging through nested JSON.
    """
    index: dict[str, dict[str, Any]] = {}

    def attrs(spec: dict) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for k in ("type", "not_null", "unique"):
            if k in spec:
                out[k] = spec[k]
        return out

    def walk_object_props(base_path: str, parent_label: str, props: dict, *, in_array_items: bool) -> None:
        for sub, subspec in props.items():
            if not isinstance(subspec, dict):
                continue
            path = f"{base_path}.{sub}"
            entry = attrs(subspec)
            entry["parent"] = parent_label
            if in_array_items:
                entry["in_array_items"] = True
            index[path] = entry
            st = subspec.get("type")
            if st == "object" and isinstance(subspec.get("properties"), dict):
                walk_object_props(path, path, subspec["properties"], in_array_items=in_array_items)
            elif st == "array" and isinstance(subspec.get("items"), dict):
                walk_items(path, subspec["items"])

    def walk_items(array_field: str, items_spec: dict) -> None:
        if not isinstance(items_spec, dict) or "type" not in items_spec:
            return
        it = items_spec["type"]
        if it in SCALAR_TYPES:
            index[f"{array_field}[]"] = {
                "type": it,
                "parent_array": array_field,
                "array_items": True,
                **{k: items_spec[k] for k in ("not_null", "unique") if k in items_spec},
            }
        elif it == "object" and isinstance(items_spec.get("properties"), dict):
            walk_object_props(array_field, array_field, items_spec["properties"], in_array_items=True)
        elif it == "array" and isinstance(items_spec.get("items"), dict):
            inner = items_spec["items"]
            iit = inner.get("type") if isinstance(inner, dict) else None
            if iit in SCALAR_TYPES:
                index[f"{array_field}[][]"] = {
                    "type": iit,
                    "parent_array": array_field,
                    "array_items": True,
                    "nested_array": True,
                }
            elif iit == "object" and isinstance(inner.get("properties"), dict):
                walk_object_props(
                    f"{array_field}[]",
                    array_field,
                    inner["properties"],
                    in_array_items=True,
                )

    for fname, spec in fields.items():
        if not isinstance(spec, dict):
            continue
        t = spec.get("type")
        if t == "object" and isinstance(spec.get("properties"), dict):
            walk_object_props(fname, fname, spec["properties"], in_array_items=False)
        elif t == "array" and isinstance(spec.get("items"), dict):
            walk_items(fname, spec["items"])

    return index


def _validate_items_shape(path: str, items: Any) -> tuple[bool, str]:
    if not isinstance(items, dict):
        return False, f"{path}: 'items' must be an object with a 'type'."
    if "type" not in items:
        return False, f"{path}: 'items' missing 'type'."
    t = items["type"]
    if t not in VALID_TYPES:
        return False, f"{path}: invalid items type '{t}'."
    if t == "array":
        if "items" not in items:
            return False, f"{path}: nested array items must declare inner 'items'."
        return _validate_items_shape(f"{path}.items", items["items"])
    if t == "object":
        props = items.get("properties")
        if not isinstance(props, dict):
            return False, f"{path}: object items must have 'properties' (object, possibly empty)."
        for nk, nv in props.items():
            if not isinstance(nv, dict):
                return False, f"{path}.properties.{nk} must be an object."
            if "type" not in nv:
                return False, f"{path}.properties.{nk} missing 'type'."
            if nv["type"] not in VALID_TYPES:
                return False, f"{path}.properties.{nk} has invalid type."
            if nv["type"] == "array":
                if "items" not in nv:
                    return False, f"{path}.properties.{nk}: array requires 'items'."
                ok, err = _validate_items_shape(f"{path}.properties.{nk}.items", nv["items"])
                if not ok:
                    return False, err
            if nv["type"] == "object":
                if "properties" not in nv or not isinstance(nv["properties"], dict):
                    return False, f"{path}.properties.{nk}: object requires 'properties'."
    return True, ""


def _validate_global_record_key(schema_dict: dict, fields: dict) -> tuple[bool, str]:
    grk = schema_dict.get("global_record_key")
    if grk is None:
        return True, ""

    field_name: str | None
    if isinstance(grk, dict):
        field_name = grk.get("field")
        policy = grk.get("policy", "uuid_v4")
    elif isinstance(grk, str):
        field_name = grk
        policy = "uuid_v4"
    else:
        return False, "global_record_key must be a string or an object with 'field'."

    if not field_name or not isinstance(field_name, str):
        return False, "global_record_key.field must be a non-empty string."

    if policy not in ("uuid_v4", "from_payload"):
        return False, "global_record_key.policy must be 'uuid_v4' or 'from_payload'."

    spec = fields.get(field_name)
    if not spec:
        return False, f"global_record_key.field '{field_name}' must exist in 'fields'."

    if spec.get("type") != "str":
        return False, f"global_record_key field '{field_name}' must have type 'str'."

    if not spec.get("not_null", False):
        return False, f"global_record_key field '{field_name}' must set not_null: true."

    if not spec.get("unique", False):
        return False, f"global_record_key field '{field_name}' must set unique: true."

    return True, ""


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

        for constraint in ["not_null", "unique"]:
            if constraint in props and not isinstance(props[constraint], bool):
                return False, f"Constraint '{constraint}' for field '{field_name}' must be a boolean."

        if props["type"] == "array":
            if "items" not in props:
                return False, f"Array field '{field_name}' must declare 'items' (shape of each element)."
            ok, err = _validate_items_shape(f"fields.{field_name}.items", props["items"])
            if not ok:
                return False, err

        if props["type"] == "object":
            if "properties" not in props or not isinstance(props["properties"], dict):
                return False, f"Object field '{field_name}' must declare 'properties' (object; use {{}} if unknown)."

            for sk, sv in props["properties"].items():
                if not isinstance(sv, dict):
                    return False, f"Field '{field_name}.properties.{sk}' must be an object."
                if "type" not in sv:
                    return False, f"Field '{field_name}.properties.{sk}' missing 'type'."
                if sv["type"] not in VALID_TYPES:
                    return False, f"Field '{field_name}.properties.{sk}' has invalid type."
                if sv["type"] == "array":
                    if "items" not in sv:
                        return False, f"Field '{field_name}.properties.{sk}' (array) requires 'items'."
                    ok, err = _validate_items_shape(
                        f"fields.{field_name}.properties.{sk}.items", sv["items"]
                    )
                    if not ok:
                        return False, err
                if sv["type"] == "object":
                    if "properties" not in sv or not isinstance(sv["properties"], dict):
                        return False, f"Field '{field_name}.properties.{sk}' (object) requires 'properties'."

    ok, err = _validate_global_record_key(schema_dict, fields)
    if not ok:
        return False, err

    return True, ""


def register_schema(schema_dict: dict) -> dict:
    """Registers the schema and returns a summary."""
    is_valid, error = validate_schema(schema_dict)
    if not is_valid:
        raise ValueError(error)

    metadata_manager.save_schema(schema_dict)
    config.apply_join_key_from_schema(schema_dict)

    fields = schema_dict["fields"]
    summary: dict[str, Any] = {
        "fields_registered": len(fields),
        "array_fields": [f for f, p in fields.items() if p["type"] == "array"],
        "object_fields": [f for f, p in fields.items() if p["type"] == "object"],
        "scalar_fields": [f for f, p in fields.items() if p["type"] not in ("array", "object")],
        "join_key": config.JOIN_KEY,
        "global_record_key_policy": config.GLOBAL_RECORD_KEY_POLICY,
        "inferred_secondary_field": config.SECONDARY_JOIN_KEY,
        "nested_schema_paths_indexed": len(metadata_manager.get_schema_nested_paths()),
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


def sql_type_for_schema_type(t: str) -> str:
    """Map schema scalar type to SQLite-style type name."""
    return {"int": "INTEGER", "float": "REAL", "bool": "INTEGER"}.get(t, "TEXT")
