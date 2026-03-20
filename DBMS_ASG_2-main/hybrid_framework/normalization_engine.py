from typing import Any
import hybrid_framework.config as config
import hybrid_framework.metadata_manager as metadata_manager


# ──────────────────────────────────────────────────────────────────────────────
# Entity / PK detection
# ──────────────────────────────────────────────────────────────────────────────

def detect_entity_identifier(field_stats: dict, schema: dict) -> str | None:
    """
    Finds the most likely natural primary key among SQL-classified fields.

    Scoring:
      Priority 1 – schema declares unique + not_null AND observed uniqueness_ratio >= 0.99
      Priority 2 – only high observed uniqueness_ratio (>= 0.99), no schema constraint

    Within each priority tier, integer fields are preferred over text.
    Returns None if no suitable candidate is found.
    """
    fields = schema.get("fields", {})
    candidates: list[tuple[int, str, str]] = []   # (priority, field, dtype)

    for field, stats in field_stats.items():
        if field not in fields:
            continue
        d_type = stats.get("dominant_type")
        if d_type in ("list", "dict"):
            continue                               # PKs must be scalar

        props = fields[field]
        is_unique_schema   = props.get("unique", False)
        is_not_null_schema = props.get("not_null", False)
        uniqueness_ratio   = stats.get("uniqueness_ratio", 0)

        if is_unique_schema and is_not_null_schema and uniqueness_ratio >= 0.99:
            candidates.append((1, field, d_type))
        elif uniqueness_ratio >= 0.99:
            candidates.append((2, field, d_type))

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x[0], 0 if x[2] == "int" else 1))
    return candidates[0][1]


# ──────────────────────────────────────────────────────────────────────────────
# Functional dependency detection  (used for 3NF decomposition)
# ──────────────────────────────────────────────────────────────────────────────

def detect_functional_dependencies(
    records: list[dict],
    sql_fields: list[str],
    sample_size: int,
) -> dict[str, set[str]]:
    """
    Detects functional dependencies A → B among *sql_fields* using a record sample.

    Improvements over the original 2NF version:
      • The JOIN_KEY (timestamp PK) is excluded from both sides of every FD.
        Every column trivially depends on it; treating it as a determinant would
        suppress all transitive-dependency detection.
      • None / null values are skipped to avoid spurious FDs caused by sparse fields.
      • The mapping cardinality check uses the sample-present pairs only, so sparse
        fields are not unfairly penalised.

    Returns {field_a: {field_b, …}} for each FD that meets config.FD_THRESHOLD.
    """
    if not records:
        return {}

    join_key = config.JOIN_KEY
    # Exclude the timestamp key from FD analysis on both sides
    analysis_fields = [f for f in sql_fields if f != join_key]

    sample = records[:sample_size]
    fds: dict[str, set[str]] = {}

    for a in analysis_fields:
        determined: set[str] = set()
        for b in analysis_fields:
            if a == b:
                continue

            # val_a → set(val_b) seen in sample
            mapping: dict[str, set[str]] = {}
            for r in sample:
                val_a = r.get(a)
                val_b = r.get(b)
                # Skip rows where either value is absent – sparse fields must not
                # create false FDs by mapping every None to itself.
                if val_a is None or val_b is None:
                    continue
                key = str(val_a)
                mapping.setdefault(key, set()).add(str(val_b))

            if not mapping:
                continue

            # FD holds when nearly every value of A maps to exactly one value of B
            unique_count = sum(1 for vals in mapping.values() if len(vals) == 1)
            if unique_count / len(mapping) >= config.FD_THRESHOLD:
                determined.add(b)

        if determined:
            fds[a] = determined

    return fds


# ──────────────────────────────────────────────────────────────────────────────
# 3NF decomposition helpers
# ──────────────────────────────────────────────────────────────────────────────

def _find_fd_clusters(
    relevant_fds: dict[str, set[str]],
    all_fields: set[str],
) -> list[set[str]]:
    """
    Groups fields into equivalence clusters where every pair of fields mutually
    determines each other (A → B *and* B → A).

    Example: student_id ↔ username → both land in the same cluster so that only
    one dimension table is created for the whole "student identity" group.

    Uses path-compressed union-find for efficiency.
    """
    parent: dict[str, str] = {f: f for f in all_fields}

    def find(x: str) -> str:
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])   # path compression
            x = parent[x]
        return x

    def union(x: str, y: str) -> None:
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    for a, determined in relevant_fds.items():
        for b in determined:
            # Only merge when the dependency is *mutual*
            if b in relevant_fds and a in relevant_fds[b]:
                union(a, b)

    clusters: dict[str, set[str]] = {}
    for f in all_fields:
        root = find(f)
        clusters.setdefault(root, set()).add(f)

    return list(clusters.values())


def _pick_cluster_pk(
    cluster: set[str],
    field_stats: dict,
    schema: dict,
) -> str | None:
    """
    Selects the best surrogate primary key for a dimension table cluster.

    Scoring rubric (higher = better):
      +4  schema marks field as unique AND not_null
      +2  observed uniqueness_ratio >= 0.95
      +1  dominant type is integer (more efficient joins than text)
    """
    schema_fields = schema.get("fields", {})
    best: str | None = None
    best_score = -1

    for f in cluster:
        props = schema_fields.get(f, {})
        d_type    = field_stats.get(f, {}).get("dominant_type", "str")
        uniqueness = field_stats.get(f, {}).get("uniqueness_ratio", 0)

        score = (
            (4 if props.get("unique") and props.get("not_null") else 0)
            + (2 if uniqueness >= 0.95 else 0)
            + (1 if d_type == "int" else 0)
        )
        if score > best_score:
            best_score = score
            best = f

    return best


# ──────────────────────────────────────────────────────────────────────────────
# 3NF decomposition — core new logic
# ──────────────────────────────────────────────────────────────────────────────

def decompose_for_3nf(
    sql_fields: list[str],
    pk_field: str | None,
    fd_groups: dict[str, set[str]],
    field_stats: dict,
    schema: dict,
) -> dict:
    """
    Eliminates transitive dependencies to reach Third Normal Form (3NF).

    A *transitive dependency* exists when a non-key field A determines another
    non-key field B:
        PK (sys_ingested_at) → A → B

    Resolution strategy
    -------------------
    1. Identify every non-key, non-join-key field that acts as a determinant.
    2. Cluster mutually-determining fields together (e.g. student_id ↔ username
       form one cluster because each uniquely identifies the other).
    3. For each cluster:
         a. Pick the best surrogate PK (integer, unique+not_null in schema).
         b. Collect every field the cluster transitively determines outside itself.
         c. Build a typed dimension table: (cluster_pk [PK], other_cluster_fields,
            external_dependencies).
         d. Remove external dependencies from the main 'records' table.
         e. Keep the cluster PK as a FK in 'records'.
    4. Return the decomposition plan consumed by build_sql_table_schema.

    Example (given the student simulation data)
    -------------------------------------------
    Detected FDs:  student_id → {username, name, email}
                   username   → {student_id, name, email}
    Cluster:       {student_id, username}
    Cluster PK:    student_id  (int, unique+not_null in schema)
    Dim table:     student_id_dim(student_id PK, username UNIQUE, name, email)
    Removed from records: username, name, email
    FK in records: student_id → student_id_dim.student_id

    Returns
    -------
    {
        "dimension_tables": {
            table_name: {
                "determinant":      str,    # PK of this dimension table
                "columns":          dict,   # {col_name: col_info}
                "primary_key":      str,
                "foreign_keys":     list,
                "dependent_fields": list,   # non-PK fields housed here
                "all_fields":       list,   # every field in this table
            }
        },
        "fields_removed_from_main": set,    # fields migrated out of 'records'
        "extra_fks":                list,   # FK descriptors to attach to 'records'
    }
    """
    join_key = config.JOIN_KEY

    # Fields that may NOT trigger a decomposition:
    #   - JOIN_KEY: it IS the PK of the records table (timestamp); every field
    #     trivially depends on it — not a transitive dependency.
    #   - pk_field: if the schema has a natural PK (e.g. student_id with
    #     uniqueness_ratio ~1.0), dependencies from it are DIRECT from the PK,
    #     not transitive.
    protected: set[str] = {join_key}
    if pk_field:
        protected.add(pk_field)

    sql_field_set = set(sql_fields)

    # ── Step 1: filter FDs to non-protected determinants with SQL-field targets ─
    relevant_fds: dict[str, set[str]] = {}
    for det, determined in fd_groups.items():
        if det in protected:
            continue
        deps = (determined & sql_field_set) - protected
        if deps:
            relevant_fds[det] = deps

    if not relevant_fds:
        return {
            "dimension_tables":         {},
            "fields_removed_from_main": set(),
            "extra_fks":                [],
        }

    # ── Step 2: gather all fields touched by any relevant FD ──────────────────
    involved: set[str] = set(relevant_fds.keys())
    for deps in relevant_fds.values():
        involved.update(deps)

    # ── Step 3: cluster mutually-determining fields (e.g. student_id ↔ username)
    clusters = _find_fd_clusters(relevant_fds, involved)

    # ── Step 4: build a dimension table per cluster ───────────────────────────
    schema_fields = schema.get("fields", {})

    def _sql_type(f: str) -> str:
        d = field_stats.get(f, {}).get("dominant_type", "str")
        return {"int": "INTEGER", "float": "REAL", "bool": "INTEGER"}.get(d, "TEXT")

    dimension_tables: dict[str, dict] = {}
    fields_removed: set[str]          = set()
    extra_fks: list[dict]             = []
    already_claimed: set[str]         = set()   # prevents double-removal across clusters

    for cluster in clusters:
        cluster_determinants = cluster & set(relevant_fds.keys())
        if not cluster_determinants:
            continue

        det_pk = _pick_cluster_pk(cluster_determinants, field_stats, schema)
        if det_pk is None:
            continue

        # Fields determined by any cluster member that are NOT in the cluster
        external_deps: set[str] = set()
        for det in cluster_determinants:
            for dep in relevant_fds[det]:
                if dep not in cluster and dep not in protected:
                    external_deps.add(dep)

        # Don't re-claim fields already assigned to an earlier dimension table
        external_deps -= already_claimed

        all_dim_fields  = cluster | external_deps
        dependent_fields = list(all_dim_fields - {det_pk})
        table_name       = f"{det_pk}_dim"

        # Build column definitions
        columns: dict[str, dict] = {}
        for f in all_dim_fields:
            props  = schema_fields.get(f, {})
            is_pk  = (f == det_pk)
            columns[f] = {
                "sql_type":    _sql_type(f),
                "primary_key": is_pk,
                "unique":      True if is_pk else props.get("unique", False),
                "not_null":    True if is_pk else props.get("not_null", False),
            }

        dimension_tables[table_name] = {
            "determinant":      det_pk,
            "columns":          columns,
            "primary_key":      det_pk,
            "foreign_keys":     [],
            "dependent_fields": dependent_fields,
            "all_fields":       list(all_dim_fields),
        }

        # Dependent fields leave 'records'; the cluster PK stays as an FK.
        to_remove = all_dim_fields - {det_pk}
        fields_removed.update(to_remove)
        already_claimed.update(all_dim_fields)

        extra_fks.append({
            "column":            det_pk,
            "references_table":  table_name,
            "references_column": det_pk,
        })

    return {
        "dimension_tables":         dimension_tables,
        "fields_removed_from_main": fields_removed,
        "extra_fks":                extra_fks,
    }


# ──────────────────────────────────────────────────────────────────────────────
# 1NF / 2NF helpers  (unchanged logic, cleaned up style)
# ──────────────────────────────────────────────────────────────────────────────

def detect_repeating_groups(
    schema: dict, field_stats: dict, records: list[dict]
) -> list[dict]:
    """
    Identifies fields that are arrays-of-objects and can be normalised into a
    child SQL table (one-to-many, joined via JOIN_KEY).

    Fields with deeply nested structures (arrays inside arrays, etc.) are left
    for MongoDB and are skipped here.
    """
    groups: list[dict] = []
    fields = schema.get("fields", {})

    for field, stats in field_stats.items():
        if field not in fields:
            continue

        placement = metadata_manager.get_placement_for_field(field)
        if (
            placement
            and placement["backend"] == "mongo"
            and placement.get("reason") != "schema_required_immediate"
        ):
            pass  # classification wins; still inspect for potential SQL promotion

        schema_type   = fields[field]["type"]
        dominant_type = stats.get("dominant_type")
        has_nested    = stats.get("has_nested", False)

        is_repeating = (
            (schema_type == "array" and dominant_type == "list")
            or (schema_type == "object" and dominant_type == "list")
            or (has_nested and dominant_type == "list")
        )
        if not is_repeating:
            continue

        sub_fields: dict[str, int] = {}
        goes_to_sql  = True
        sample_count = 0

        for r in records[:50]:
            if not (field in r and isinstance(r[field], list)):
                continue
            for item in r[field]:
                if isinstance(item, dict):
                    sample_count += 1
                    for sk, sv in item.items():
                        sub_fields[sk] = sub_fields.get(sk, 0) + 1
                        if isinstance(sv, (dict, list)):
                            goes_to_sql = False
                else:
                    goes_to_sql = False

        if sample_count == 0:
            goes_to_sql = False

        if goes_to_sql:
            groups.append({
                "parent_field":     field,
                "child_table_name": field,
                "sub_fields":       list(sub_fields.keys()),
                "goes_to_sql":      True,
            })

    return groups


def detect_nested_objects(
    schema: dict, field_stats: dict, records: list[dict]
) -> list[dict]:
    """
    Finds fields declared as 'object' in the schema that are observed as dicts.
    Decides whether to inline them (≤ MAX_INLINE_FIELDS scalar sub-fields),
    extract to a separate SQL table, or defer to MongoDB.
    """
    nested: list[dict] = []
    fields = schema.get("fields", {})

    for field, stats in field_stats.items():
        if field not in fields:
            continue
        if not (fields[field]["type"] == "object" and stats.get("dominant_type") == "dict"):
            continue

        sub_fields: dict[str, int] = {}
        all_scalar = True

        for r in records[:50]:
            if not (field in r and isinstance(r[field], dict)):
                continue
            for sk, sv in r[field].items():
                sub_fields[sk] = sub_fields.get(sk, 0) + 1
                if isinstance(sv, (dict, list)):
                    all_scalar = False

        if len(sub_fields) <= config.MAX_INLINE_FIELDS and all_scalar:
            strategy = "inline"
        elif all_scalar:
            strategy = "separate_table"
        else:
            strategy = "mongo"

        nested.append({
            "field":      field,
            "strategy":   strategy,
            "sub_fields": [f"{field}.{sk}" for sk in sub_fields.keys()],
        })

    return nested


# ──────────────────────────────────────────────────────────────────────────────
# SQL schema assembly
# ──────────────────────────────────────────────────────────────────────────────

def build_sql_table_schema(
    sql_fields: list[str],
    field_stats: dict,
    schema: dict,
    pk_field: str | None,
    fd_groups: dict[str, set[str]],
    repeating_groups: list[dict],
    nested_objects: list[dict],
) -> dict:
    """
    Builds the complete SQL table schema, integrating all three normal forms:

      1NF – atomic columns (enforced by type coercion in ingest.py)
      2NF – partial-dependency elimination via repeating-group child tables
            and nested-object extraction
      3NF – transitive-dependency elimination via decompose_for_3nf(), which
            extracts non-key determinant clusters into typed dimension tables
            (e.g. student_id_dim) and replaces the dependent columns in
            'records' with a single FK column.

    The FD groups computed by detect_functional_dependencies() are consumed here
    for the first time — this was the missing link that kept the original code
    in 2NF.

    Returns
    -------
    {
        "tables":                {table_name: table_info},
        "flattened_objects":     {orig_field: [dot_notation_cols]},
        "dimension_tables_meta": {table_name: {determinant, dependent_fields, all_fields}},
    }
    """
    main_table = "records"
    tables: dict[str, dict] = {
        main_table: {"columns": {}, "primary_key": None, "foreign_keys": []}
    }
    flattened_objects: dict[str, list[str]] = {}

    # Fields already claimed by repeating groups / nested object tables
    handled_fields: set[str] = set()
    for rg in repeating_groups:
        handled_fields.add(rg["parent_field"])
    for no in nested_objects:
        handled_fields.add(no["field"])

    def _map_type(f: str) -> str:
        d = field_stats.get(f, {}).get("dominant_type", "str")
        return {"int": "INTEGER", "float": "REAL", "bool": "INTEGER"}.get(d, "TEXT")

    # ── 3NF decomposition (the critical step missing in 2NF) ──────────────────
    decomp         = decompose_for_3nf(sql_fields, pk_field, fd_groups, field_stats, schema)
    dim_tables     = decomp["dimension_tables"]
    fields_removed = decomp["fields_removed_from_main"]   # leave 'records' entirely
    extra_fks      = decomp["extra_fks"]

    # Anything handled by 2NF normalisations OR 3NF decomposition is excluded
    # from direct placement in the main 'records' table.
    excluded_from_main = handled_fields | fields_removed

    # ── Main 'records' table columns ──────────────────────────────────────────
    for field in sql_fields:
        if field in excluded_from_main or field == config.JOIN_KEY:
            continue
        props = schema.get("fields", {}).get(field, {})
        tables[main_table]["columns"][field] = {
            "sql_type":    _map_type(field),
            "primary_key": field == pk_field,
            "unique":      props.get("unique", False),
            "not_null":    props.get("not_null", False),
        }

    # The system join key is always present in 'records'
    if config.JOIN_KEY not in tables[main_table]["columns"]:
        tables[main_table]["columns"][config.JOIN_KEY] = {
            "sql_type":    "TEXT",
            "unique":      True,
            "not_null":    True,
            "primary_key": pk_field is None,
        }

    # Set primary key
    if pk_field and pk_field in tables[main_table]["columns"]:
        tables[main_table]["primary_key"] = pk_field
    else:
        tables[main_table]["primary_key"] = config.JOIN_KEY

    # Attach FK declarations pointing to dimension tables
    tables[main_table]["foreign_keys"].extend(extra_fks)

    # ── 2NF: nested object tables ─────────────────────────────────────────────
    for no in nested_objects:
        if no["strategy"] == "inline":
            flattened_objects[no["field"]] = no["sub_fields"]
            for sf in no["sub_fields"]:
                tables[main_table]["columns"][sf] = {
                    "sql_type": "TEXT", "unique": False, "not_null": False
                }

        elif no["strategy"] == "separate_table":
            tname = no["field"]
            tables[tname] = {
                "columns": {
                    "_row_id":                 {"sql_type": "INTEGER", "primary_key": True},
                    config.JOIN_KEY:           {"sql_type": "TEXT",    "not_null": True},
                    config.SECONDARY_JOIN_KEY: {"sql_type": "TEXT"},
                },
                "primary_key": "_row_id",
                "foreign_keys": [{
                    "column":            config.JOIN_KEY,
                    "references_table":  main_table,
                    "references_column": config.JOIN_KEY,
                }],
            }
            if pk_field and pk_field in tables[main_table]["columns"]:
                tables[tname]["columns"][pk_field] = {
                    "sql_type": _map_type(pk_field), "not_null": True
                }
            for sf in no["sub_fields"]:
                tables[tname]["columns"][sf] = {"sql_type": "TEXT"}

    # ── 2NF: repeating group (child) tables ───────────────────────────────────
    for rg in repeating_groups:
        if not rg["goes_to_sql"]:
            continue
        tname = rg["child_table_name"]
        tables[tname] = {
            "columns": {
                "_row_id":                 {"sql_type": "INTEGER", "primary_key": True},
                config.JOIN_KEY:           {"sql_type": "TEXT",    "not_null": True},
                config.SECONDARY_JOIN_KEY: {"sql_type": "TEXT"},
            },
            "primary_key": "_row_id",
            "foreign_keys": [{
                "column":            config.JOIN_KEY,
                "references_table":  main_table,
                "references_column": config.JOIN_KEY,
            }],
        }
        if pk_field and pk_field in tables[main_table]["columns"]:
            tables[tname]["columns"][pk_field] = {
                "sql_type": _map_type(pk_field), "not_null": True
            }
        for sf in rg["sub_fields"]:
            tables[tname]["columns"][sf] = {"sql_type": "TEXT"}

    # ── 3NF: dimension tables ─────────────────────────────────────────────────
    for dim_name, dim_info in dim_tables.items():
        tables[dim_name] = {
            "columns":      dim_info["columns"],
            "primary_key":  dim_info["primary_key"],
            "foreign_keys": dim_info["foreign_keys"],
        }

    # Metadata consumed by crud.py to route inserts / reads through dimension tables
    dimension_tables_meta = {
        name: {
            "determinant":      info["determinant"],
            "dependent_fields": info["dependent_fields"],
            "all_fields":       info["all_fields"],
        }
        for name, info in dim_tables.items()
    }

    return {
        "tables":                tables,
        "flattened_objects":     flattened_objects,
        "dimension_tables_meta": dimension_tables_meta,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Top-level entry point
# ──────────────────────────────────────────────────────────────────────────────

def run_normalization(all_records: list[dict], field_stats: dict, schema: dict) -> dict:
    """
    Orchestrates the full 1NF → 2NF → 3NF normalization pipeline:

      1. Derive the SQL-classified field list from current field placement.
      2. Detect the natural entity identifier (candidate PK).
      3. Detect functional dependencies across SQL fields.  ← fed into 3NF
      4. Detect repeating groups (1NF/2NF child tables).
      5. Detect nested objects (inline columns or separate tables).
      6. Build the complete SQL schema with 3NF decomposition applied.
      7. Persist: SQL tables, 3NF dimension-table metadata, updated placements.
    """
    sql_fields = [
        f for f in field_stats
        if (
            metadata_manager.get_placement_for_field(f) is not None
            and metadata_manager.get_placement_for_field(f)["backend"] == "sql"
        )
    ]

    pk_field         = detect_entity_identifier(field_stats, schema)
    fd_groups        = detect_functional_dependencies(all_records, sql_fields, config.FD_SAMPLE_SIZE)
    repeating_groups = detect_repeating_groups(schema, field_stats, all_records)
    nested_objects   = detect_nested_objects(schema, field_stats, all_records)

    result = build_sql_table_schema(
        sql_fields, field_stats, schema,
        pk_field, fd_groups, repeating_groups, nested_objects,
    )

    # Persist the full SQL schema. save_sql_tables expects the full result dict
    # {"tables": ..., "flattened_objects": ..., "dimension_tables_meta": ...}
    # so it can save both sql_tables and flattened_objects in one atomic write.
    metadata_manager.save_sql_tables(result)

    # Persist 3NF semantics so crud.py can route inserts / JOINs correctly
    metadata_manager.save_3nf_dimension_tables(result.get("dimension_tables_meta", {}))

    # ── Update field placements to reflect all normalisation decisions ─────────
    new_placements: dict[str, dict] = {}

    # 2NF: repeating groups → child table
    for rg in repeating_groups:
        if rg["goes_to_sql"]:
            new_placements[rg["parent_field"]] = {
                "backend": "sql",
                "table":   rg["child_table_name"],
                "reason":  "repeating_group_normalization",
            }

    # 2NF: nested objects → inline in records or separate table
    for no in nested_objects:
        if no["strategy"] != "mongo":
            new_placements[no["field"]] = {
                "backend":  "sql",
                "table":    "records",
                "strategy": no["strategy"],
                "reason":   "nested_object_normalization",
            }

    # 3NF: dimension table fields
    dim_meta = result.get("dimension_tables_meta", {})
    for dim_name, dim_info in dim_meta.items():
        det = dim_info["determinant"]
        # FK column: stays in 'records', but logically owned by the dimension table
        new_placements[det] = {
            "backend":         "sql",
            "table":           "records",
            "dimension_table": dim_name,
            "reason":          "3nf_foreign_key",
        }
        # Dependent fields: moved entirely to the dimension table
        for f in dim_info["dependent_fields"]:
            new_placements[f] = {
                "backend": "sql",
                "table":   dim_name,
                "reason":  "3nf_decomposition",
            }

    if new_placements:
        # Merge INTO existing placements rather than replacing them.
        # save_field_placement overwrites the entire map, so we must re-read the
        # placements that classification.classify_fields() wrote earlier and layer
        # the normalisation decisions on top.  Without this merge, every field
        # that was not involved in a normalisation step loses its sql/mongo
        # classification and gets silently routed to the buffer on the next insert.
        existing_placements = metadata_manager.get_field_placement()
        metadata_manager.save_field_placement({**existing_placements, **new_placements})

    return result
