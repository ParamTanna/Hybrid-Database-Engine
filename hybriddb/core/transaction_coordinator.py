import json
import os
import threading
import time
from datetime import datetime, timezone
from copy import deepcopy
from typing import Any

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from hybriddb.config import paths
from hybriddb.core import sql_db
from hybriddb.ingestion.classification import _main_table_name
from hybriddb.storage.audit_store import touch_updated, upsert_created
from hybriddb.crud.delete_operation import execute_delete
from hybriddb.crud.insert_operation import flatten, validate
from hybriddb.crud.read_operation import execute_read


SCHEMA_FILE = paths.SCHEMA_FILE


class TransactionCoordinator:
    """Coordinates ACID writes across PostgreSQL and MongoDB.

    Cross-backend writes use a two-phase commit when MongoDB is a replica set
    (multi-document transactions available): all PostgreSQL work is staged in an
    open transaction, all MongoDB work is performed inside a Mongo transaction,
    then PostgreSQL commits first and MongoDB commits last. A failure before the
    PostgreSQL commit rolls both sides back atomically; a failure of the final
    MongoDB commit triggers a converge step that restores PostgreSQL so the two
    backends never diverge. When transactions are unavailable (standalone mongod)
    it falls back to a snapshot-and-compensate scheme.

    Concurrent writes to the same record are serialised with a per-key lock, and
    metadata reclassification holds a process-wide lock so DDL never interleaves
    with a coordinated write.
    """

    def __init__(
        self,
        metadata_file: str | None = None,
        mongo_uri: str | None = None,
        mongo_db: str | None = None,
    ):
        self.metadata_file = metadata_file or paths.METADATA_FILE
        self.mongo_uri = mongo_uri or paths.MONGO_URI
        self.mongo_db = mongo_db or paths.MONGO_DB_NAME

        # One reusable client (required to host sessions / transactions).
        self._client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=2000)

        # Concurrency control.
        self._lock_map_guard = threading.Lock()
        self._key_locks: dict[Any, threading.Lock] = {}
        self._reclassify_lock = threading.Lock()
        self._mongo_txn_supported: bool | None = None  # lazily probed

    # ------------------------------------------------------------------
    # Concurrency helpers
    # ------------------------------------------------------------------
    def _key_lock(self, key_value) -> threading.Lock:
        """Return the lock guarding writes to a single global-key value."""
        with self._lock_map_guard:
            lock = self._key_locks.get(key_value)
            if lock is None:
                lock = threading.Lock()
                self._key_locks[key_value] = lock
            return lock

    def _mongo_supports_txn(self) -> bool:
        """True if MongoDB is a replica-set member (transactions available)."""
        if self._mongo_txn_supported is None:
            try:
                hello = self._client.admin.command("hello")
                self._mongo_txn_supported = bool(hello.get("setName"))
            except Exception:
                self._mongo_txn_supported = False
        return self._mongo_txn_supported

    @staticmethod
    def _commit_mongo_with_retry(session, attempts: int = 3) -> None:
        """Commit a Mongo transaction, retrying only the safe commit-unknown case."""
        last = None
        for _ in range(attempts):
            try:
                session.commit_transaction()
                return
            except PyMongoError as exc:
                last = exc
                if exc.has_error_label("UnknownTransactionCommitResult"):
                    continue
                raise
        if last is not None:
            raise last

    def execute(self, query: dict) -> dict:
        """
        Main entry point. Routes to the correct coordinated operation.
        query must have an "operation" key: insert | update | delete | read

        Returns:
        {
            "success": True | False,
            "operation": "insert" | "update" | "delete" | "read",
            "message": "human readable result",
            "rolled_back": True | False,
            "data": <result for read operations, None for writes>
        }
        """
        op = str(query.get("operation", "")).lower()
        result = {
            "success": False,
            "operation": op,
            "message": "Unknown operation",
            "rolled_back": False,
            "data": None,
        }

        try:
            meta = self._load_meta()

            if op == "insert":
                return self._coordinated_insert(query, meta)
            if op == "update":
                return self._coordinated_update(query, meta)
            if op == "delete":
                return self._coordinated_delete(query, meta)
            if op == "read":
                return self._coordinated_read(query, meta)

            result["message"] = "Unsupported operation. Use insert, update, delete, or read."
            return result
        except Exception as exc:
            return {
                "success": False,
                "operation": op,
                "message": f"Coordinator failure: {exc}",
                "rolled_back": False,
                "data": None,
            }

    def _load_meta(self) -> dict:
        """Load metadata_store.json. Raise RuntimeError if missing."""
        if not os.path.exists(self.metadata_file):
            raise RuntimeError(f"Metadata file missing: {self.metadata_file}")
        with open(self.metadata_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def _check_backends(self) -> tuple[bool, bool]:
        """
        Returns (sql_available, mongo_available).
        Check PostgreSQL with a SELECT 1; check MongoDB with a ping. Also
        refreshes whether Mongo transactions are available.
        """
        sql_available = sql_db.health_check()

        mongo_available = False
        try:
            self._client.admin.command("ping")
            mongo_available = True
        except Exception:
            mongo_available = False

        # Refresh transaction-support probe while we're here.
        if mongo_available:
            self._mongo_txn_supported = None
            self._mongo_supports_txn()

        return sql_available, mongo_available

    def _routing_snapshot(self, meta: dict) -> dict[str, tuple[str, str]]:
        out: dict[str, tuple[str, str]] = {}
        for fname, fmeta in (meta.get("fields") or {}).items():
            out[fname] = (
                fmeta.get("storage_backend") or "Buffer",
                fmeta.get("storage_detail") or "Buffer",
            )
        return out

    def _field_children_map(self, meta: dict) -> dict[str, dict[str, str]]:
        fields = meta.get("fields", {})
        out: dict[str, dict[str, str]] = {}
        for fname, fmeta in fields.items():
            parent = fmeta.get("parent")
            if not parent:
                continue
            out.setdefault(parent, {})[fname.split(".")[-1]] = fname
        return out

    def _coerce_scalar_strict(self, field_name: str, expected_type: str, value: Any):
        # Keep input values as provided; schema checks are handled elsewhere.
        return value

    @staticmethod
    def _coercible(value, ftype: str) -> bool:
        """Mirror of the insert validator's coercion test: can `value` be safely
        turned into `ftype`? Used to detect type drift."""
        if ftype == "string":
            return True
        if ftype == "boolean":
            return isinstance(value, bool)
        if ftype == "int":
            if isinstance(value, bool):
                return False
            if isinstance(value, int):
                return True
            try:
                int(value)
                return True
            except Exception:
                return False
        if ftype == "float":
            if isinstance(value, bool):
                return False
            if isinstance(value, (int, float)):
                return True
            try:
                float(value)
                return True
            except Exception:
                return False
        return True  # object/array/unknown handled elsewhere

    def _apply_type_conflict_policy(self, data: dict, meta: dict) -> tuple[dict, str | None]:
        """Resolve values that cannot be coerced to their field's declared type.

        Safe/representational mismatches (e.g. 12345 -> "12345", "42" -> 42) are
        coerced later by the validator and are NOT touched here. This only acts
        on genuinely un-coercible scalar values, per paths.TYPE_CONFLICT_POLICY:

          "adaptive" -> widen the field to a schemaless Mongo field (preserve the
                        value; the global key is never widened).
          "strict"   -> reject the write with a clear error message.

        Returns (meta, error). On "strict" with a conflict, error is the message
        and meta is unchanged; otherwise error is None (meta may be reloaded if
        fields were widened).
        """
        if not isinstance(data, dict):
            return meta, None
        fields = meta.get("fields", {})
        global_key = meta.get("global_key")
        drifted = []
        for key, value in data.items():
            fmeta = fields.get(key)
            if not fmeta or key == global_key or value is None:
                continue
            if fmeta.get("type_widened"):
                continue
            ftype = fmeta.get("type", "string")
            if ftype not in ("int", "float", "boolean"):
                continue
            if not self._coercible(value, ftype):
                drifted.append((key, ftype, value))

        if not drifted:
            return meta, None

        if paths.TYPE_CONFLICT_POLICY == "strict":
            key, ftype, value = drifted[0]
            return meta, (
                f"Field '{key}' expects {ftype}, got {type(value).__name__} "
                f"({value!r}). Rejected by strict type-conflict policy."
            )

        # Adaptive: widen each drifted field to schemaless Mongo, preserving data.
        from hybriddb.core.reclassify_migrate import widen_field_to_mongo

        with self._reclassify_lock:
            for key, _ftype, _value in drifted:
                try:
                    widen_field_to_mongo(meta, key)
                except Exception as exc:
                    print(f"[TC] type-widen failed for '{key}': {exc}")
        # Reload so the rest of the operation sees the new routing + types.
        return self._load_meta(), None

    def _coerce_known_field_value(
        self,
        field_name: str,
        value: Any,
        meta: dict,
        strict_unknown_nested: bool,
    ):
        fields = meta.get("fields", {})
        child_map = self._field_children_map(meta)
        fmeta = fields.get(field_name, {})
        ftype = fmeta.get("type", "string")

        if ftype not in ("object", "array"):
            return value

        if ftype == "object":
            if not isinstance(value, dict):
                raise ValueError(
                    f"Field '{field_name}' expects object, got {type(value).__name__} ({value!r})."
                )
            out = {}
            cmap = child_map.get(field_name, {})
            for key, subval in value.items():
                child_full = cmap.get(key)
                if child_full is None:
                    if strict_unknown_nested:
                        raise ValueError(f"Field '{field_name}.{key}' does not exist in schema.")
                    out[key] = subval
                    continue
                out[key] = self._coerce_known_field_value(
                    child_full,
                    subval,
                    meta,
                    strict_unknown_nested,
                )
            return out

        # array
        if not isinstance(value, list):
            raise ValueError(
                f"Field '{field_name}' expects array, got {type(value).__name__} ({value!r})."
            )
        cmap = child_map.get(field_name, {})
        out_list = []
        for item in value:
            if isinstance(item, dict) and cmap:
                new_item = {}
                for key, subval in item.items():
                    child_full = cmap.get(key)
                    if child_full is None:
                        if strict_unknown_nested:
                            raise ValueError(f"Field '{field_name}.{key}' does not exist in schema.")
                        new_item[key] = subval
                        continue
                    new_item[key] = self._coerce_known_field_value(
                        child_full,
                        subval,
                        meta,
                        strict_unknown_nested,
                    )
                out_list.append(new_item)
            else:
                out_list.append(item)
        return out_list

    def _coerce_insert_data_strict(self, data: dict, meta: dict) -> dict:
        fields = meta.get("fields", {})
        out = {}
        for key, value in data.items():
            if key in fields:
                out[key] = self._coerce_known_field_value(key, value, meta, strict_unknown_nested=True)
            else:
                raise ValueError(f"Field '{key}' does not exist in schema.")
        return out

    def _coerce_update_data_strict(self, data: Any, meta: dict, entity: str | None) -> Any:
        fields = meta.get("fields", {})
        child_map = self._field_children_map(meta)

        if entity:
            if entity not in fields:
                raise ValueError(f"Field '{entity}' does not exist in schema.")

            emeta = fields.get(entity, {})
            etype = emeta.get("type")
            if isinstance(data, dict) and etype in ("array", "object"):
                out = {}
                cmap = child_map.get(entity, {})
                for key, value in data.items():
                    child_full = key if key in fields else cmap.get(key)
                    if child_full is None:
                        raise ValueError(f"Field '{entity}.{key}' does not exist in schema.")
                    out[key] = self._coerce_known_field_value(
                        child_full,
                        value,
                        meta,
                        strict_unknown_nested=True,
                    )
                return out

            return self._coerce_known_field_value(entity, data, meta, strict_unknown_nested=True)

        if not isinstance(data, dict):
            return data

        out = {}
        for key, value in data.items():
            if key not in fields:
                raise ValueError(f"Field '{key}' does not exist in schema.")
            out[key] = self._coerce_known_field_value(key, value, meta, strict_unknown_nested=True)
        return out

    def _coerce_where_values(self, where: dict, meta: dict) -> dict:
        fields = meta.get("fields", {})
        out = {}
        for key, value in where.items():
            fmeta = fields.get(key)
            if not fmeta:
                raise ValueError(f"Field '{key}' does not exist in schema.")

            out[key] = value

        return out

    def _collect_touched_fields(self, data: Any, meta: dict, entity: str | None = None) -> set[str]:
        fields = meta.get("fields", {})
        child_map = self._field_children_map(meta)
        touched: set[str] = set()

        def _walk(full_name: str, value: Any):
            if full_name not in fields:
                return
            touched.add(full_name)
            fmeta = fields.get(full_name, {})
            ftype = fmeta.get("type")

            if ftype == "object" and isinstance(value, dict):
                cmap = child_map.get(full_name, {})
                for ck, cv in value.items():
                    child_full = cmap.get(ck)
                    if child_full:
                        _walk(child_full, cv)
            elif ftype == "array":
                cmap = child_map.get(full_name, {})
                if isinstance(value, dict):
                    for ck, cv in value.items():
                        child_full = cmap.get(ck)
                        if child_full:
                            _walk(child_full, cv)
                elif isinstance(value, list):
                    for item in value:
                        if not isinstance(item, dict):
                            continue
                        for ck, cv in item.items():
                            child_full = cmap.get(ck)
                            if child_full:
                                _walk(child_full, cv)

        if entity:
            _walk(entity, data)
            return touched

        if not isinstance(data, dict):
            return touched

        for key, value in data.items():
            if key in fields:
                _walk(key, value)

        return touched

    def _build_routing_rows(
        self,
        touched_fields: set[str],
        before_map: dict[str, tuple[str, str]],
        after_map: dict[str, tuple[str, str]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for fname in sorted(touched_fields):
            old_backend, old_detail = before_map.get(fname, ("Buffer", "Buffer"))
            new_backend, new_detail = after_map.get(fname, ("Buffer", "Buffer"))
            rows.append(
                {
                    "field": fname,
                    "before_backend": old_backend,
                    "before_detail": old_detail,
                    "after_backend": new_backend,
                    "after_detail": new_detail,
                    "moved": (old_backend, old_detail) != (new_backend, new_detail),
                }
            )
        return rows

    def _unknown_fields_for_update(self, data: Any, meta: dict, entity: str | None = None) -> list[str]:
        """Return unknown update-field paths that are not declared in metadata."""
        fields = meta.get("fields", {})

        def _child_map(parent: str) -> dict[str, str]:
            out = {}
            for fname, fmeta in fields.items():
                if fmeta.get("parent") == parent:
                    out[fname.split(".")[-1]] = fname
            return out

        def _walk(full_name: str, value: Any) -> list[str]:
            fmeta = fields.get(full_name)
            if not fmeta:
                return [full_name]

            ftype = fmeta.get("type")
            unknown: list[str] = []

            if ftype == "object" and isinstance(value, dict):
                cmap = _child_map(full_name)
                for ck, cv in value.items():
                    child_full = cmap.get(ck)
                    if not child_full:
                        unknown.append(f"{full_name}.{ck}")
                    else:
                        unknown.extend(_walk(child_full, cv))

            elif ftype == "array":
                cmap = _child_map(full_name)
                if isinstance(value, dict):
                    # Entity-scoped array update payload (item patch form)
                    for ck, cv in value.items():
                        child_full = cmap.get(ck)
                        if not child_full:
                            unknown.append(f"{full_name}.{ck}")
                        else:
                            unknown.extend(_walk(child_full, cv))
                elif isinstance(value, list):
                    # Full replacement payload for array entities
                    for item in value:
                        if not isinstance(item, dict):
                            continue
                        for ck, cv in item.items():
                            child_full = cmap.get(ck)
                            if not child_full:
                                unknown.append(f"{full_name}.{ck}")
                            else:
                                unknown.extend(_walk(child_full, cv))

            return unknown

        unknown: list[str] = []

        if entity is not None:
            emeta = fields.get(entity)
            if not emeta:
                return [entity]

            if isinstance(data, dict) and emeta.get("type") in ("array", "object"):
                cmap = _child_map(entity)
                for k, v in data.items():
                    full = k if k in fields else cmap.get(k)
                    if not full:
                        unknown.append(f"{entity}.{k}")
                    else:
                        unknown.extend(_walk(full, v))
            else:
                unknown.extend(_walk(entity, data))

            return sorted(set(unknown))

        if not isinstance(data, dict):
            return []

        for k, v in data.items():
            full = k if k in fields else None
            if not full:
                unknown.append(k)
                continue
            unknown.extend(_walk(full, v))

        return sorted(set(unknown))

    def _unknown_where_fields_for_update(
        self,
        where: dict,
        meta: dict,
        entity: str | None = None,
    ) -> list[str]:
        """Return unknown where-clause keys for update operations."""
        fields = meta.get("fields", {})
        global_key = meta.get("global_key")

        entity_child_leafs: set[str] = set()
        if entity:
            for fname, fmeta in fields.items():
                if fmeta.get("parent") == entity:
                    entity_child_leafs.add(fname.split(".")[-1])

        unknown = []
        for key in where.keys():
            if key == global_key:
                continue
            if key in fields:
                continue
            if entity and key in entity_child_leafs:
                continue
            unknown.append(key)

        return sorted(set(unknown))

    def _collect_schema_unique_paths(self, fields: dict, parent: str = "") -> set[str]:
        unique_paths = set()
        for name, definition in fields.items():
            if not isinstance(definition, dict):
                continue
            full = f"{parent}.{name}" if parent else name
            if definition.get("unique", False):
                unique_paths.add(full)
            nested = definition.get("fields")
            if isinstance(nested, dict):
                unique_paths |= self._collect_schema_unique_paths(nested, full)
        return unique_paths

    def _schema_identifier_fields(self, meta: dict) -> set[str]:
        """Return unique identifier fields from schema unique flags and SQL PKs."""
        identifiers = set()
        global_key = meta.get("global_key")
        if global_key:
            identifiers.add(global_key)

        if os.path.exists(SCHEMA_FILE):
            try:
                with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
                    raw_schema = json.load(f)
                field_block = {k: v for k, v in raw_schema.items() if k != "global_key"}
                identifiers |= self._collect_schema_unique_paths(field_block)
            except Exception:
                pass

        fields = meta.get("fields", {})
        km_sql = meta.get("key_management", {}).get("SQL", {})
        for table, info in km_sql.items():
            pk = info.get("primary_key")
            if not pk:
                continue
            matched = False
            for fname, fmeta in fields.items():
                if (
                    fmeta.get("storage_backend") == "SQL"
                    and fmeta.get("storage_detail") == f"SQL.{table}"
                    and fname.split(".")[-1] == pk
                ):
                    identifiers.add(fname)
                    matched = True
            if not matched:
                identifiers.add(pk)

        return identifiers

    def _resolve_global_key_from_where(self, where: dict, meta: dict):
        """
        Resolve global_key value from where clause.
        Supports direct global_key or any schema-unique/primary-key field.
        Returns (gk_value, normalized_where, error_message).
        """
        global_key = meta.get("global_key")
        if not global_key:
            return None, where, "Metadata missing global_key."

        def _path_get(obj: Any, dotted_path: str):
            cur = obj
            for part in dotted_path.split("."):
                if not isinstance(cur, dict) or part not in cur:
                    return None
                cur = cur[part]
            return cur

        identifiers = self._schema_identifier_fields(meta)

        # Support both exact identifier paths and unambiguous leaf-name aliases.
        ident_by_leaf: dict[str, str] = {}
        ambiguous_leafs: set[str] = set()
        for ident in identifiers:
            leaf = ident.split(".")[-1]
            if leaf in ident_by_leaf and ident_by_leaf[leaf] != ident:
                ambiguous_leafs.add(leaf)
            else:
                ident_by_leaf[leaf] = ident

        resolved_candidates: list[tuple[str, str]] = []  # (where_key, identifier_path)
        for k in where.keys():
            if k in identifiers:
                resolved_candidates.append((k, k))
                continue
            if k in ambiguous_leafs:
                continue
            ident = ident_by_leaf.get(k)
            if ident:
                resolved_candidates.append((k, ident))

        # Deduplicate identifier paths while preserving order.
        candidate_fields: list[tuple[str, str]] = []
        seen_ident_paths = set()
        for where_key, ident_path in resolved_candidates:
            if ident_path in seen_ident_paths:
                continue
            seen_ident_paths.add(ident_path)
            candidate_fields.append((where_key, ident_path))

        # If global_key is explicitly provided, all other provided unique
        # identifiers must refer to the same record.
        if global_key in where:
            gk_val = where.get(global_key)
            base_rows = execute_read(
                {
                    "operation": "read",
                    "fields": ["*"],
                    "where": {global_key: gk_val},
                },
                meta,
            )
            if not base_rows:
                return None, where, f"No record found for {global_key}={gk_val}."

            base_record = base_rows[0]
            for where_key, ident_path in candidate_fields:
                if ident_path == global_key:
                    continue
                expected = where.get(where_key)
                actual = _path_get(base_record, ident_path)
                if actual != expected:
                    return (
                        None,
                        where,
                        f"No record found matching provided identifiers: {global_key} and {where_key}.",
                    )

            return gk_val, where, None

        if not candidate_fields:
            return (
                None,
                where,
                f"Update requires '{global_key}' or another unique identifier in where.",
            )

        normalized_where = dict(where)
        resolved_gk = None
        used_where_keys: list[str] = []
        for where_key, ident_path in candidate_fields:
            value = where.get(where_key)
            rows = execute_read(
                {
                    "operation": "read",
                    "fields": [global_key],
                    "where": {ident_path: value},
                },
                meta,
            )
            if not rows:
                return None, where, f"No record found for provided unique identifier '{where_key}'."
            if len(rows) > 1:
                return None, where, f"Identifier '{where_key}' is not unique in stored data."

            gk_val = rows[0].get(global_key)
            if resolved_gk is None:
                resolved_gk = gk_val
            elif resolved_gk != gk_val:
                return None, where, "Provided unique identifiers refer to different records."

            # Remove alternate identifier keys to avoid breaking entity-item matching.
            used_where_keys.append(where_key)
            for ident_key in used_where_keys:
                if ident_key != global_key:
                    normalized_where.pop(ident_key, None)
            normalized_where[global_key] = resolved_gk

        if resolved_gk is not None:
            return resolved_gk, normalized_where, None

        return None, where, "No record found for provided unique identifier(s)."

    def _record_matches_where(self, record: dict, where: dict) -> bool:
        """Return True when every where key/value matches the record payload."""

        def _path_get(obj: Any, dotted_path: str):
            cur = obj
            for part in dotted_path.split("."):
                if not isinstance(cur, dict) or part not in cur:
                    return None
                cur = cur[part]
            return cur

        for key, expected in where.items():
            actual = _path_get(record, key) if "." in key else record.get(key)

            # Support simple list filters: where field IN [a, b, ...]
            if isinstance(expected, list):
                if actual not in expected:
                    return False
            else:
                if actual != expected:
                    return False

        return True

    # ------------------------------------------------------------------
    # Cross-backend atomic execution (two-phase commit + fallback)
    # ------------------------------------------------------------------
    def _safe_converge(self, converge) -> bool:
        if converge is None:
            return True
        try:
            converge()
            return True
        except Exception as exc:
            print(f"[TC] Converge/rollback warning: {exc}")
            return False

    def _execute_atomic(self, *, sql_stage, mongo_work, converge=None) -> dict:
        """Run a cross-backend write atomically.

        sql_stage(cur)          : stage all PostgreSQL writes on the cursor (no commit)
        mongo_work(db, session) : perform all MongoDB writes (session=None in fallback)
        converge()              : restore a consistent state when the two backends
                                  may have diverged (post-Postgres-commit Mongo
                                  failure, or a fallback partial write).

        Returns {"ok", "rolled_back", "error", "diverged"}.
        """
        conn = sql_db.connect()
        pg_committed = False
        use_txn = self._mongo_supports_txn()
        session = None
        try:
            with conn.cursor() as cur:
                sql_stage(cur)

            db = self._client[self.mongo_db]
            if use_txn:
                session = self._client.start_session()
                session.start_transaction()
                mongo_work(db, session)
                conn.commit()
                pg_committed = True
                self._commit_mongo_with_retry(session)
            else:
                mongo_work(db, None)
                conn.commit()
                pg_committed = True

            return {"ok": True, "rolled_back": False, "error": None, "diverged": False}

        except Exception as exc:
            if session is not None:
                try:
                    if session.in_transaction:
                        session.abort_transaction()
                except Exception:
                    pass

            if not pg_committed:
                try:
                    conn.rollback()
                except Exception:
                    pass
                if use_txn:
                    # Both sides rolled back atomically — no divergence.
                    return {"ok": False, "rolled_back": True,
                            "error": str(exc), "diverged": False}
                # Fallback: non-transactional Mongo may have written partially.
                ok = self._safe_converge(converge)
                return {"ok": False, "rolled_back": True,
                        "error": str(exc) + ("" if ok else " WARNING: rollback incomplete"),
                        "diverged": True}

            # Postgres committed but Mongo commit failed -> converge to remove divergence.
            ok = self._safe_converge(converge)
            return {"ok": False, "rolled_back": True,
                    "error": str(exc) + ("" if ok else " WARNING: rollback incomplete"),
                    "diverged": True}
        finally:
            if session is not None:
                try:
                    session.end_session()
                except Exception:
                    pass
            try:
                sql_db.release(conn)
            except Exception:
                pass

    def _stage_sql_insert(self, cur, data: dict, meta: dict, gk_val) -> None:
        """Stage the SQL portion of a record onto an open cursor (no commit).

        Inserts the main-table scalar row and any SQL child-table array rows.
        """
        meta_fields = meta.get("fields", {})
        global_key_field = meta.get("global_key")
        if global_key_field is None:
            raise RuntimeError("Metadata missing global_key")
        km_sql = meta.get("key_management", {}).get("SQL", {})

        # Scalar SQL fields (level 0, not array/object).
        sql_scalars = {}
        for fname, fmeta in meta_fields.items():
            if (
                fmeta.get("storage_backend") == "SQL"
                and fmeta.get("level") == 0
                and fmeta.get("type") not in ("array", "object")
                and fname in data
            ):
                sql_scalars[fname] = data[fname]
        if global_key_field in data:
            sql_scalars[global_key_field] = data[global_key_field]

        main_table = _main_table_name(global_key_field)
        if sql_scalars and main_table in km_sql:
            sql_db.insert(cur, main_table, sql_scalars)

        # Array fields routed to SQL child tables.
        for fname, fmeta in meta_fields.items():
            if (
                fmeta.get("storage_backend") == "SQL"
                and fmeta.get("type") == "array"
                and fname in data
                and isinstance(data[fname], list)
            ):
                child_table = fname
                if child_table not in km_sql:
                    continue
                for item in data[fname]:
                    if not isinstance(item, dict):
                        continue
                    sql_db.insert(cur, child_table, {global_key_field: gk_val, **item})

    def _coordinated_insert(self, query: dict, meta: dict) -> dict:
        try:
            before_routing = self._routing_snapshot(meta)
            sql_ok, mongo_ok = self._check_backends()
            if not (sql_ok and mongo_ok):
                return self._failure("insert", "Prepare failed: one or more backends unavailable.")

            data = query.get("data") or {}
            global_key = meta.get("global_key")
            if global_key is None:
                return self._failure("insert", "Metadata missing global_key.")

            # Type-conflict policy: un-coercible values either widen the field to
            # Mongo (adaptive) or reject the write (strict). Safe coercions are
            # left to the validator. Skipped during update's internal re-insert.
            if not query.get("_skip_widen", False):
                meta, type_err = self._apply_type_conflict_policy(data, meta)
                if type_err:
                    return self._failure("insert", type_err, rolled_back=False)

            try:
                data = self._coerce_insert_data_strict(data, meta)
            except ValueError as exc:
                return self._failure("insert", f"Validation failed: {exc}", rolled_back=False)

            gk_val = data.get(global_key)
            if gk_val is None:
                return self._failure("insert", f"Insert requires '{global_key}' in data.")

            if not query.get("_skip_existing_check", False):
                existing = execute_read(
                    {
                        "operation": "read",
                        "fields": ["*"],
                        "where": {global_key: gk_val},
                    },
                    meta,
                )
                if existing:
                    return self._failure(
                        "insert",
                        f"Record already exists for {global_key}={gk_val}.",
                        rolled_back=False,
                    )

            if not query.get("_skip_validation", False):
                # Validate before touching any backend
                try:
                    original_data = deepcopy(data)
                    flat = flatten(data)
                    validated_data, warnings = validate(flat, meta)
                    if warnings:
                        for w in warnings:
                            print(f"[TC] Warning: {w}")
                    # Keep structured payload for writes; apply validated scalar coercions.
                    data = original_data
                    for key, value in validated_data.items():
                        if "." not in key:
                            data[key] = value
                except ValueError as exc:
                    print(f"[TC] Validation FAILED: {exc}")
                    msg = str(exc)
                    if "record already exists" in msg.lower():
                        return self._failure(
                            "insert",
                            msg,
                            rolled_back=False,
                        )
                    return self._failure(
                        "insert",
                        f"Validation failed: {exc}",
                        rolled_back=False,
                    )

            print(f"[TC] Starting coordinated insert for {global_key}={gk_val}")

            def _sql_stage(cur):
                self._stage_sql_insert(cur, data, meta, gk_val)

            def _mongo_work(db, session):
                self._mongo_insert(db, data, meta, gk_val, session=session)

            def _converge():
                # Remove any half-written record (no prior state for an insert).
                with sql_db.transaction() as (_c, _cur):
                    self._sql_delete_by_key(_cur, meta, gk_val)
                self._with_mongo_db(lambda db: self._mongo_delete_by_key(db, meta, gk_val))

            with self._key_lock(gk_val):
                res = self._execute_atomic(
                    sql_stage=_sql_stage, mongo_work=_mongo_work, converge=_converge
                )

            if not res["ok"]:
                err = res["error"] or ""
                if "unique" in err.lower() or "duplicate" in err.lower():
                    return self._failure(
                        "insert",
                        "Record already exists: duplicate unique/primary key value.",
                        rolled_back=False,
                    )
                return self._failure(
                    "insert", f"Insert failed: {err}", rolled_back=res["rolled_back"]
                )

            print("[TC] SQL + Mongo committed")

            # Keep metadata classification in sync after successful writes.
            migrated_fields: list[str] = []
            if not query.get("_skip_reclassify", False):
                try:
                    from hybriddb.core.reclassify_migrate import check_and_migrate

                    with self._reclassify_lock:
                        meta, migrated_fields = check_and_migrate(meta)
                except Exception as exc:
                    print(f"[TC] Reclassification warning: {exc}")

            now_utc = datetime.now(timezone.utc).isoformat()
            try:
                upsert_created(global_key, gk_val, now_utc)
            except Exception as exc:
                print(f"[TC] Audit timestamp warning (insert): {exc}")

            touched_fields = self._collect_touched_fields(data, meta)
            after_routing = self._routing_snapshot(meta)
            routing_rows = self._build_routing_rows(touched_fields, before_routing, after_routing)
            routing_event = {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "operation": "insert",
                "global_key": global_key,
                "global_key_value": gk_val,
                "entity": None,
                "migrated_fields": sorted(set(migrated_fields)),
                "routing": routing_rows,
            }
            print("[TC] Transaction SUCCESS")
            return self._success("insert", "Coordinated insert committed.", data={"routing": routing_event})

        except Exception as exc:
            return self._failure("insert", f"Insert coordination error: {exc}")

    def _mongo_insert(self, db, data: dict, meta: dict, gk_val, session=None):
        """Insert MongoDB-bound fields into the appropriate collections.

        All writes join `session` (a Mongo transaction) when provided.
        """
        global_key = meta.get("global_key")
        if global_key is None:
            raise RuntimeError("Metadata missing global_key")
        km_mongo = meta.get("key_management", {}).get("Mongo", {})
        meta_fields = meta.get("fields", {})
        main_collection = _main_table_name(global_key)

        # Embedded fields go into the main document
        embed_doc = {global_key: gk_val}
        for fname, fmeta in meta_fields.items():
            backend = fmeta.get("storage_backend", "")
            detail = fmeta.get("storage_detail", "")
            if (
                (backend == "Mongo.embed" or backend == "Mongo.document")
                or detail in ("Mongo.embed", "Mongo.document")
            ) and fname in data:
                embed_doc[fname] = data[fname]

        if len(embed_doc) > 1:
            db[main_collection].replace_one(
                {global_key: gk_val}, embed_doc, upsert=True, session=session
            )

        # Reference arrays go into separate collections
        ref_tops = [f for f in km_mongo.get("reference", []) if "." not in f]
        for fname in ref_tops:
            if fname not in data or not isinstance(data[fname], list):
                continue
            db[fname].delete_many({global_key: gk_val}, session=session)
            docs = [{global_key: gk_val, **item} for item in data[fname] if isinstance(item, dict)]
            if docs:
                db[fname].insert_many(docs, session=session)

    def _coordinated_update(self, query: dict, meta: dict) -> dict:
        """
        Coordinated update across SQL + MongoDB.
        Update strategy is delete-then-insert (matching existing system design).
        """
        try:
            before_routing = self._routing_snapshot(meta)
            sql_ok, mongo_ok = self._check_backends()
            if not (sql_ok and mongo_ok):
                return self._failure(
                    "update",
                    "Prepare failed: one or more backends unavailable.",
                    rolled_back=False,
                )

            global_key = meta.get("global_key")
            where = query.get("where") or {}
            entity = query.get("entity")

            # Treat invalid/scalar entity input as a regular full-record update.
            if entity:
                fmeta = meta.get("fields", {}).get(entity)
                if not fmeta or fmeta.get("type") not in ("array", "object"):
                    query = dict(query)
                    query.pop("entity", None)

            if not where:
                return self._failure(
                    "update",
                    "Update requires a non-empty where clause.",
                    rolled_back=False,
                )

            try:
                where = self._coerce_where_values(where, meta)
            except ValueError as exc:
                return self._failure("update", f"Validation failed: {exc}", rolled_back=False)

            # Type-conflict policy on a full-record update (entity updates target
            # nested data and are skipped here).
            if not query.get("entity"):
                meta, type_err = self._apply_type_conflict_policy(query.get("data") or {}, meta)
                if type_err:
                    return self._failure("update", type_err, rolled_back=False)

            try:
                coerced_data = self._coerce_update_data_strict(query.get("data", {}), meta, query.get("entity"))
            except ValueError as exc:
                return self._failure("update", f"Validation failed: {exc}", rolled_back=False)

            gk_val, normalized_where, resolve_err = self._resolve_global_key_from_where(where, meta)
            if resolve_err:
                return self._failure("update", resolve_err, rolled_back=False)

            query = dict(query)
            query["where"] = normalized_where
            query["data"] = coerced_data
            where = normalized_where

            unknown_where = self._unknown_where_fields_for_update(where, meta, query.get("entity"))
            if unknown_where:
                return self._failure(
                    "update",
                    "Field does not exist in schema: " + ", ".join(unknown_where),
                    rolled_back=False,
                )

            print(f"[TC] Starting coordinated update for {global_key}={gk_val}")

            read_query = {"operation": "read", "fields": ["*"], "where": {global_key: gk_val}}
            old_records = execute_read(read_query, meta)
            if not old_records:
                return self._failure(
                    "update",
                    f"No record found for {global_key}={gk_val}.",
                    rolled_back=False,
                )

            if not self._record_matches_where(old_records[0], where):
                return self._failure(
                    "update",
                    "No record found matching provided identifiers.",
                    rolled_back=False,
                )

            old_snapshot = deepcopy(old_records)
            sql_before = self._snapshot_sql_rows(meta, [gk_val])
            mongo_before = self._snapshot_mongo_docs(meta, [gk_val])

            unknown = self._unknown_fields_for_update(query.get("data", {}), meta, query.get("entity"))
            if unknown:
                return self._failure(
                    "update",
                    "Field does not exist in schema: " + ", ".join(unknown),
                    rolled_back=False,
                )

            # Build a FULL merged record so non-entity fields are preserved.
            if query.get("entity"):
                entity = query.get("entity")
                update_data = query.get("data", {})
                merged_record = deepcopy(old_records[0])
                extra_where = {k: v for k, v in where.items() if k != global_key}

                current_value = merged_record.get(entity)
                if isinstance(current_value, list):
                    if extra_where:
                        new_items = []
                        matched = False
                        for item in current_value:
                            if isinstance(item, dict) and all(item.get(k) == v for k, v in extra_where.items()):
                                merged_item = dict(item)
                                if isinstance(update_data, dict):
                                    merged_item.update(update_data)
                                new_items.append(merged_item)
                                matched = True
                            else:
                                new_items.append(item)
                        if not matched:
                            return self._failure(
                                "update",
                                f"No item in '{entity}' matched where conditions {extra_where}.",
                                rolled_back=False,
                            )
                        merged_record[entity] = new_items
                    else:
                        # Replace whole entity array/object content.
                        merged_record[entity] = update_data
                else:
                    # Non-list entity (object/scalar) replacement.
                    merged_record[entity] = update_data

                merged_record[global_key] = gk_val
            else:
                # Non-entity update: reuse existing merge helper.
                from hybriddb.crud.update_operation import build_merged_record

                merged_record = build_merged_record(old_records[0], query, meta)

            # execute_read may attach user-facing audit fields that are not part
            # of metadata schema; strip unknown top-level keys before re-insert.
            schema_fields = set((meta.get("fields") or {}).keys())
            merged_record = {
                k: v for k, v in merged_record.items()
                if k in schema_fields
            }

            # Atomic delete-then-insert of the merged record across both backends.
            # The full delete + re-insert happens inside ONE Postgres transaction
            # and ONE Mongo transaction, so a failure cannot leave the record
            # half-written or divergent between backends.
            def _sql_stage(cur):
                self._sql_delete_by_key(cur, meta, gk_val)
                self._stage_sql_insert(cur, merged_record, meta, gk_val)

            def _mongo_work(db, session):
                self._mongo_delete_by_key(db, meta, gk_val, session=session)
                self._mongo_insert(db, merged_record, meta, gk_val, session=session)

            def _converge():
                # Restore the pre-update state on both backends.
                self._rollback_to_snapshot(
                    meta, [gk_val], old_snapshot, sql_before, mongo_before
                )

            with self._key_lock(gk_val):
                res = self._execute_atomic(
                    sql_stage=_sql_stage, mongo_work=_mongo_work, converge=_converge
                )

            if not res["ok"]:
                return self._failure(
                    "update", f"Update failed: {res['error']}", rolled_back=res["rolled_back"]
                )

            migrated_fields: list[str] = []
            try:
                from hybriddb.core.reclassify_migrate import check_and_migrate

                with self._reclassify_lock:
                    meta, migrated_fields = check_and_migrate(meta)
            except Exception as exc:
                print(f"[TC] Reclassification warning: {exc}")

            now_utc = datetime.now(timezone.utc).isoformat()
            try:
                gk_name = meta.get("global_key")
                if isinstance(gk_name, str) and gk_name:
                    touch_updated(gk_name, gk_val, now_utc)
            except Exception as exc:
                print(f"[TC] Audit timestamp warning (update): {exc}")

            entity_name = query.get("entity")
            touched_fields = self._collect_touched_fields(query.get("data", {}), meta, entity_name)
            if isinstance(entity_name, str) and entity_name:
                touched_fields.add(entity_name)
            after_routing = self._routing_snapshot(meta)
            routing_rows = self._build_routing_rows(touched_fields, before_routing, after_routing)
            routing_event = {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "operation": "update",
                "global_key": global_key,
                "global_key_value": gk_val,
                "entity": query.get("entity"),
                "migrated_fields": sorted(set(migrated_fields)),
                "routing": routing_rows,
            }
            print("[TC] Transaction SUCCESS")
            return self._success("update", "Coordinated update committed.", data={"routing": routing_event})

        except Exception as exc:
            return self._failure("update", f"Update coordination error: {exc}")

    def _coordinated_delete(self, query: dict, meta: dict) -> dict:
        """
        Coordinated delete across SQL + MongoDB.
        """
        try:
            sql_ok, mongo_ok = self._check_backends()
            if not (sql_ok and mongo_ok):
                return self._failure(
                    "delete",
                    "Prepare failed: one or more backends unavailable.",
                    rolled_back=False,
                )

            where = query.get("where") or {}
            global_key = meta.get("global_key")
            if not where:
                return self._failure(
                    "delete",
                    "Delete requires a non-empty where clause.",
                    rolled_back=False,
                )

            try:
                where = self._coerce_where_values(where, meta)
            except ValueError as exc:
                return self._failure("delete", f"Validation failed: {exc}", rolled_back=False)

            gk_val, normalized_where, resolve_err = self._resolve_global_key_from_where(where, meta)
            if resolve_err:
                return self._failure("delete", resolve_err, rolled_back=False)

            query = dict(query)
            query["where"] = normalized_where

            # Preserve legacy delete semantics for field-wide and entity-scoped
            # deletes (they are not full-record key deletes).
            if query.get("field") is not None or query.get("entity") is not None:
                ok = execute_delete(query, meta, _auto_confirm=True)
                if ok:
                    return self._success("delete", "Delete completed.")
                return self._failure("delete", "Delete failed.", rolled_back=False)

            gk_raw = normalized_where.get(global_key)
            gk_values = gk_raw if isinstance(gk_raw, list) else [gk_raw]
            print(f"[TC] Starting coordinated delete for {global_key}={gk_values}")

            snapshot = self._read_snapshot_by_key(meta, gk_values)
            existing_keys = {
                rec.get(global_key)
                for rec in snapshot
                if isinstance(rec, dict) and rec.get(global_key) is not None
            }
            missing_keys = [val for val in gk_values if val not in existing_keys]
            if missing_keys:
                if len(missing_keys) == 1:
                    return self._failure(
                        "delete",
                        f"No record found for {global_key}={missing_keys[0]}.",
                        rolled_back=False,
                    )
                return self._failure(
                    "delete",
                    f"No record found for {global_key} value(s): {missing_keys}.",
                    rolled_back=False,
                )

            # Ensure every requested record also satisfies all provided where keys.
            snapshot_by_key = {
                rec.get(global_key): rec
                for rec in snapshot
                if isinstance(rec, dict) and rec.get(global_key) is not None
            }
            non_matching_keys = [
                key_val
                for key_val in gk_values
                if not self._record_matches_where(snapshot_by_key.get(key_val, {}), normalized_where)
            ]
            if non_matching_keys:
                return self._failure(
                    "delete",
                    "No record found matching provided identifiers.",
                    rolled_back=False,
                )

            sql_before = self._snapshot_sql_rows(meta, gk_values)
            mongo_before = self._snapshot_mongo_docs(meta, gk_values)

            del_key = gk_values if len(gk_values) > 1 else gk_values[0]

            # Atomic delete across both backends (Postgres txn + Mongo txn).
            def _sql_stage(cur):
                self._sql_delete_by_key(cur, meta, del_key)

            def _mongo_work(db, session):
                self._mongo_delete_by_key(db, meta, del_key, session=session)

            def _converge():
                # Restore the deleted records on both backends.
                self._rollback_to_snapshot(meta, gk_values, snapshot, sql_before, mongo_before)

            # Serialise on all affected keys to avoid interleaving with writes.
            locks = [self._key_lock(v) for v in gk_values]
            for lk in locks:
                lk.acquire()
            try:
                res = self._execute_atomic(
                    sql_stage=_sql_stage, mongo_work=_mongo_work, converge=_converge
                )
            finally:
                for lk in reversed(locks):
                    lk.release()

            if not res["ok"]:
                return self._failure(
                    "delete", f"Delete failed: {res['error']}", rolled_back=res["rolled_back"]
                )

            print("[TC] Transaction SUCCESS")

            if snapshot:
                try:
                    from hybriddb.core.reclassify_migrate import check_and_migrate

                    with self._reclassify_lock:
                        meta, _ = check_and_migrate(meta)
                except Exception as exc:
                    print(f"[TC] Reclassification warning: {exc}")

            return self._success("delete", "Coordinated delete committed.")

        except Exception as exc:
            return self._failure("delete", f"Delete coordination error: {exc}")

    def _coordinated_read(self, query: dict, meta: dict) -> dict:
        """
        Read is non-destructive so no coordination needed.
        Just call execute_read and wrap in the standard result dict.
        Strip internal keys (_id, unknown_top, discarded, received_at)
        from results before returning.
        """
        try:
            where = query.get("where") or {}
            if where:
                try:
                    coerced_where = self._coerce_where_values(where, meta)
                    query = dict(query)
                    query["where"] = coerced_where
                except ValueError as exc:
                    return self._failure("read", f"Validation failed: {exc}")

            records = execute_read(query, meta)
            return {
                "success": True,
                "operation": "read",
                "message": "Read successful.",
                "rolled_back": False,
                "data": self._strip_internal(records),
            }
        except Exception as exc:
            return self._failure("read", f"Read failed: {exc}")

    def _sql_delete_by_key(self, cur, meta: dict, global_key_value):
        """Delete all rows matching global_key from all SQL tables.

        Operates on an open cursor `cur` (psycopg). Child tables are deleted
        before the main table to respect foreign keys.
        """
        try:
            global_key = meta["global_key"]
            km_sql = meta.get("key_management", {}).get("SQL", {})
            if not km_sql:
                return

            values = global_key_value if isinstance(global_key_value, list) else [global_key_value]
            if not values:
                return
            placeholders = ",".join(["%s"] * len(values))

            child_tables = [t for t, i in km_sql.items() if i.get("foreign_key")]
            for table in child_tables:
                cur.execute(
                    f"DELETE FROM {table} WHERE {global_key} IN ({placeholders})",
                    values,
                )

            main_table = _main_table_name(global_key)
            if main_table in km_sql:
                cur.execute(
                    f"DELETE FROM {main_table} WHERE {global_key} IN ({placeholders})",
                    values,
                )
            else:
                # No identifiable main table: delete child tables first, others after.
                remaining = [t for t in km_sql.keys() if t not in child_tables]
                for table in remaining:
                    cur.execute(
                        f"DELETE FROM {table} WHERE {global_key} IN ({placeholders})",
                        values,
                    )
        except Exception as exc:
            raise RuntimeError(f"SQL delete by key failed: {exc}")

    def _mongo_delete_by_key(self, db, meta: dict, global_key_value, session=None):
        """Delete all documents matching global_key from all Mongo collections."""
        try:
            global_key = meta["global_key"]
            km_mongo = meta.get("key_management", {}).get("Mongo", {})
            main_collection = _main_table_name(global_key)
            ref_cols = [f for f in km_mongo.get("reference", []) if "." not in f]

            values = global_key_value if isinstance(global_key_value, list) else [global_key_value]
            mongo_filter = {global_key: {"$in": values}}

            db[main_collection].delete_many(mongo_filter, session=session)
            for col in ref_cols:
                db[col].delete_many(mongo_filter, session=session)
        except Exception as exc:
            raise RuntimeError(f"Mongo delete by key failed: {exc}")

    def _sql_restore_snapshot(self, snapshot: list[dict], meta: dict):
        """
        Re-insert SQL rows from snapshot captured before the operation.
        Used during rollback of delete and update operations.
        """
        try:
            if not snapshot:
                return
            global_key = meta["global_key"]
            gk_values = [r.get(global_key) for r in snapshot if isinstance(r, dict) and r.get(global_key) is not None]
            sql_snapshot = self._snapshot_sql_rows(meta, gk_values)
            if not self._restore_sql_row_snapshot(sql_snapshot, meta):
                raise RuntimeError("SQL row snapshot restore failed")
        except Exception as exc:
            raise RuntimeError(f"SQL snapshot restore failed: {exc}")

    def _mongo_restore_snapshot(self, snapshot: list[dict], meta: dict):
        """
        Re-insert MongoDB documents from snapshot.
        Used during rollback of delete and update operations.
        """
        try:
            if not snapshot:
                return
            global_key = meta["global_key"]
            gk_values = [r.get(global_key) for r in snapshot if isinstance(r, dict) and r.get(global_key) is not None]
            mongo_snapshot = self._snapshot_mongo_docs(meta, gk_values)
            if not self._restore_mongo_doc_snapshot(mongo_snapshot, meta):
                raise RuntimeError("Mongo doc snapshot restore failed")
        except Exception as exc:
            raise RuntimeError(f"Mongo snapshot restore failed: {exc}")

    def _failure(self, operation: str, message: str, rolled_back: bool = False) -> dict:
        return {
            "success": False,
            "operation": operation,
            "message": message,
            "rolled_back": rolled_back,
            "data": None,
        }

    def _success(self, operation: str, message: str, data: Any = None) -> dict:
        return {
            "success": True,
            "operation": operation,
            "message": message,
            "rolled_back": False,
            "data": data,
        }

    def _strip_internal(self, obj: Any):
        if isinstance(obj, dict):
            blocked = {"_id", "unknown_top", "discarded", "received_at"}
            return {k: self._strip_internal(v) for k, v in obj.items() if k not in blocked}
        if isinstance(obj, list):
            return [self._strip_internal(x) for x in obj]
        return obj

    def _with_mongo_db(self, fn):
        """Run fn(db) against the shared MongoClient (no per-call connect/close)."""
        db = self._client[self.mongo_db]
        return fn(db)

    def _read_snapshot_by_key(self, meta: dict, key_values: list[Any]) -> list[dict]:
        global_key = meta["global_key"]
        out: list[dict] = []
        for key_val in key_values:
            q = {"operation": "read", "fields": ["*"], "where": {global_key: key_val}}
            try:
                out.extend(execute_read(q, meta) or [])
            except Exception:
                continue
        return out

    def _snapshot_sql_rows(self, meta: dict, key_values: list[Any]) -> dict:
        snapshot: dict[str, list[dict]] = {}
        global_key = meta["global_key"]
        km_sql = meta.get("key_management", {}).get("SQL", {})
        if not km_sql or not key_values or not sql_db.health_check():
            return snapshot

        placeholders = ",".join(["%s"] * len(key_values))
        conn = sql_db.dict_connect(autocommit=True)
        try:
            for table in km_sql.keys():
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"SELECT * FROM {table} WHERE {global_key} IN ({placeholders})",
                            list(key_values),
                        )
                        snapshot[table] = [dict(r) for r in cur.fetchall()]
                except Exception:
                    snapshot[table] = []
                    # Clear the aborted transaction state for the next table.
                    try:
                        conn.rollback()
                    except Exception:
                        pass
        finally:
            sql_db.release(conn)

        return snapshot

    def _snapshot_mongo_docs(self, meta: dict, key_values: list[Any]) -> dict:
        snapshot: dict[str, list[dict]] = {}
        global_key = meta["global_key"]
        km_mongo = meta.get("key_management", {}).get("Mongo", {})
        main_collection = _main_table_name(global_key)
        ref_cols = [f for f in km_mongo.get("reference", []) if "." not in f]

        def _capture(db):
            mongo_filter = {global_key: {"$in": key_values}}
            snapshot[main_collection] = list(db[main_collection].find(mongo_filter, {"_id": 0}))
            for col in ref_cols:
                snapshot[col] = list(db[col].find(mongo_filter, {"_id": 0}))

        try:
            self._with_mongo_db(_capture)
        except Exception:
            return {}

        return snapshot

    def _restore_sql_row_snapshot(self, sql_snapshot: dict, meta: dict) -> bool:
        km_sql = meta.get("key_management", {}).get("SQL", {})
        if not km_sql:
            return True
        global_key = meta["global_key"]

        try:
            with sql_db.transaction() as (conn, cur):
                restored_tables = []
                for table, rows in sql_snapshot.items():
                    if not rows:
                        continue
                    info = km_sql.get(table, {})
                    pk = info.get("primary_key") or global_key
                    for row in rows:
                        conflict = [pk] if pk in row else list(row.keys())
                        sql_db.upsert(cur, table, dict(row), conflict_cols=conflict, update=True)
                    restored_tables.append(table)
                # Advance identity sequences past any explicit surrogate keys.
                for table in restored_tables:
                    sql_db.fix_identity_sequences(cur, table)
            return True
        except Exception as exc:
            print(f"[TC] SQL snapshot restore failed: {exc}")
            return False

    def _restore_mongo_doc_snapshot(self, mongo_snapshot: dict, meta: dict) -> bool:
        global_key = meta["global_key"]
        if not mongo_snapshot:
            return True

        def _restore(db):
            for collection, docs in mongo_snapshot.items():
                if not docs:
                    continue
                for doc in docs:
                    key_val = doc.get(global_key)
                    if key_val is None:
                        continue
                    db[collection].replace_one(
                        {global_key: key_val, **self._mongo_identity_filter(collection, doc, meta)},
                        doc,
                        upsert=True,
                    )

        try:
            self._with_mongo_db(_restore)
            return True
        except Exception:
            return False

    def _mongo_identity_filter(self, collection: str, doc: dict, meta: dict) -> dict:
        global_key = meta["global_key"]
        if collection == _main_table_name(global_key):
            return {}

        # For reference collections, include all keys except global_key for stable matching.
        return {k: v for k, v in doc.items() if k != global_key}

    def _restore_logical_snapshot(self, snapshot: list[dict], meta: dict) -> bool:
        if not snapshot:
            return True
        all_ok = True
        for record in snapshot:
            ins = self._coordinated_insert(
                {"operation": "insert", "data": record, "_skip_reclassify": True},
                meta,
            )
            if not ins.get("success"):
                all_ok = False
        return all_ok

    def _rollback_to_snapshot(
        self,
        meta: dict,
        key_values: list[Any],
        logical_snapshot: list[dict],
        sql_snapshot: dict,
        mongo_snapshot: dict,
    ) -> bool:
        global_key = meta["global_key"]
        rollback_ok = True

        # Clear current state for keys in both backends, then restore pre-state.
        try:
            with sql_db.transaction() as (conn, cur):
                self._sql_delete_by_key(cur, meta, key_values)
            print("[TC] SQL rollback clear complete")
        except Exception:
            rollback_ok = False

        try:
            self._with_mongo_db(lambda db: self._mongo_delete_by_key(db, meta, key_values))
            print("[TC] MongoDB rollback clear complete")
        except Exception:
            rollback_ok = False

        # Always restore SQL directly from SQL row snapshots so rollback can
        # recover core state even if MongoDB is unavailable.
        restored_sql = self._restore_sql_row_snapshot(sql_snapshot, meta)

        # Restore Mongo docs when possible. If Mongo is unavailable this stays
        # false and we report incomplete rollback, but SQL data is still recovered.
        restored_mongo = self._restore_mongo_doc_snapshot(mongo_snapshot, meta)

        # Fallback to logical snapshot only when SQL row snapshot restore fails.
        if not restored_sql and logical_snapshot:
            restored_sql = self._restore_logical_snapshot(logical_snapshot, meta)

        rollback_ok = rollback_ok and restored_sql and restored_mongo

        if rollback_ok:
            print("[TC] Rollback complete")
        else:
            print("[TC] WARNING: rollback incomplete")

        return rollback_ok


def run_quick_test():
    """
    Quick sanity test — insert a record, read it back, update it, delete it.
    Prints PASS or FAIL for each step.
    Run with: python transaction_coordinator.py
    """
    tc = TransactionCoordinator()

    unique_id = int(time.time())
    insert_query = {
        "operation": "insert",
        "data": {
            "customer_id": unique_id,
            "email": f"tc_{unique_id}@example.com",
            "name": "TC Test",
            "age": 30,
        },
    }

    read_query = {
        "operation": "read",
        "fields": ["*"],
        "where": {"customer_id": unique_id},
    }

    update_query = {
        "operation": "update",
        "where": {"customer_id": unique_id},
        "data": {"name": "TC Test Updated"},
    }

    delete_query = {
        "operation": "delete",
        "where": {"customer_id": unique_id},
    }

    try:
        r1 = tc.execute(insert_query)
        print("INSERT:", "PASS" if r1.get("success") else "FAIL", "-", r1.get("message"))

        r2 = tc.execute(read_query)
        read_ok = r2.get("success") and bool(r2.get("data"))
        print("READ:", "PASS" if read_ok else "FAIL", "-", r2.get("message"))

        r3 = tc.execute(update_query)
        print("UPDATE:", "PASS" if r3.get("success") else "FAIL", "-", r3.get("message"))

        r4 = tc.execute(delete_query)
        print("DELETE:", "PASS" if r4.get("success") else "FAIL", "-", r4.get("message"))

    except Exception as exc:
        print("Quick test execution error:", exc)


if __name__ == "__main__":
    run_quick_test()
