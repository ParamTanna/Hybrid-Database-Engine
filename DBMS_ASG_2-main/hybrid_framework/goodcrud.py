import json
import logging
import sys
from typing import Any
from sqlalchemy import create_engine, text
from pymongo import MongoClient
import pymongo.errors
import hybrid_framework.config as config
import hybrid_framework.metadata_manager as metadata_manager

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


class CRUDManager:
    def __init__(self):
        # config.SQL_URL may be a relative sqlite path (e.g. "sqlite:///./data/hybrid.db").
        # Relative paths are resolved against the CWD at create_engine() call time, which may
        # differ from the CWD at ingest time — causing split-brain where inserts and reads hit
        # different physical files.  Always resolve to an absolute path anchored at the directory
        # that contains crud.py so the same file is used no matter where the process is launched.
        sql_url = config.SQL_URL
        if sql_url.startswith("sqlite:///") and not sql_url.startswith("sqlite:////"):
            rel = sql_url[len("sqlite:///"):]
            import os as _os
            if not _os.path.isabs(rel):
                from pathlib import Path as _Path
                abs_path = (_Path(__file__).resolve().parent / rel).resolve()
                abs_path.parent.mkdir(parents=True, exist_ok=True)
                sql_url = f"sqlite:///{abs_path}"
        print(f"[INIT] CRUDManager starting — SQL_URL={sql_url}")
        self.sql_engine = create_engine(sql_url)
        try:
            self.mongo_client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=2000)
            self.mongo_db     = self.mongo_client[config.MONGO_DB]
            self.mongo_client.server_info()
            self.mongo_available = True
            print(f"[INIT] MongoDB connected — URI={config.MONGO_URI}  DB={config.MONGO_DB}")
        except (pymongo.errors.ConnectionFailure, pymongo.errors.ServerSelectionTimeoutError):
            print("[INIT] MongoDB unavailable — running in SQL-only mode")
            logger.warning("MongoDB unavailable. SQL-only mode.")
            self.mongo_available = False

    # ──────────────────────────────────────────────────────────────────────────
    # DDL helpers
    # ──────────────────────────────────────────────────────────────────────────

    def ensure_all_tables(self, sql_tables: dict) -> None:
        """Create or ALTER SQL tables to match the current schema metadata."""
        print(f"\n[DDL] ensure_all_tables — tables to sync: {list(sql_tables.keys())}")
        with self.sql_engine.connect() as conn:
            for table_name, info in sql_tables.items():
                # Query sqlite_master on THIS connection so we always see the
                # current committed state, including tables created earlier in
                # this same loop.
                exists = conn.execute(
                    text("SELECT 1 FROM sqlite_master WHERE type='table' AND name=:n"),
                    {"n": table_name},
                ).fetchone() is not None

                if not exists:
                    cols: list[str] = []
                    pk = info.get("primary_key")
                    for col_name, col_info in info["columns"].items():
                        col_def = f'"{col_name}" {col_info["sql_type"]}'
                        if col_name == pk:
                            col_def += " PRIMARY KEY"
                        if col_info.get("not_null"):
                            col_def += " NOT NULL"
                        if col_info.get("unique") and col_name != pk:
                            col_def += " UNIQUE"
                        cols.append(col_def)
                    if not pk:
                        cols.append("_row_id INTEGER PRIMARY KEY AUTOINCREMENT")
                    print(f"  [DDL] CREATE TABLE '{table_name}' ({len(info['columns'])} columns, PK={pk or '_row_id'})")
                    conn.execute(text(f'CREATE TABLE "{table_name}" ({", ".join(cols)})'))
                    conn.commit()
                else:
                    # Fetch existing column names on the same connection via PRAGMA.
                    # PRAGMA table_info returns rows: (cid, name, type, notnull, dflt, pk)
                    existing_cols = {
                        row[1] for row in conn.execute(
                            text(f'PRAGMA table_info("{table_name}")')
                        ).all()
                    }
                    new_cols = [c for c in info["columns"] if c not in existing_cols]
                    if new_cols:
                        print(f"  [DDL] ALTER TABLE '{table_name}': adding columns {new_cols}")
                    for col_name, col_info in info["columns"].items():
                        if col_name not in existing_cols:
                            conn.execute(text(
                                f'ALTER TABLE "{table_name}" ADD COLUMN '
                                f'"{col_name}" {col_info["sql_type"]}'
                            ))
                            conn.commit()
                    if not new_cols:
                        print(f"  [DDL] '{table_name}' already up-to-date ({len(existing_cols)} columns)")

    # ──────────────────────────────────────────────────────────────────────────
    # Type coercion
    # ──────────────────────────────────────────────────────────────────────────

    def _value_to_sql(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        if isinstance(value, bool):
            return 1 if value else 0
        return value

    @staticmethod
    def _safe_key(k: str) -> str:
        """
        Sanitise a column name for use as a SQLAlchemy named bind parameter.

        SQLAlchemy's :named_param syntax treats a dot as attribute access, so
        dot-notation column names from flattened nested objects (e.g.
        'address.city') must have their dots replaced before being embedded in
        a parameterised query.  Double-underscore is chosen as the separator
        to avoid collisions with ordinary snake_case field names.

        The column name itself is always quoted ("address.city") so the DB
        stores it correctly; only the *bind-param label* needs sanitising.
        """
        return k.replace(".", "__")

    # ──────────────────────────────────────────────────────────────────────────
    # INSERT
    # ──────────────────────────────────────────────────────────────────────────

    def insert_record(
        self,
        record: dict,
        sql_tables: dict,
        mongo_collections: dict,
        field_placement: dict,
        flattened_objects: dict,
    ) -> None:
        """
        Splits one normalised record across all backends, respecting 3NF layout.

        3NF insert order (within a single SQL transaction):
          1. Dimension tables first  – INSERT OR IGNORE so repeated FK values
             (same student ingested many times) never raise a constraint error.
          2. Main 'records' table    – the dependent fields have been moved to
             dimension tables, so they are omitted here; only the FK column
             (determinant) remains.
          3. Child tables (2NF)      – arrays-of-objects written as separate rows.
          4. MongoDB collections     – embedded or referenced documents.
        """
        sys_ingested_at = record.get(config.JOIN_KEY)
        username        = record.get(config.SECONDARY_JOIN_KEY)

        print(f"\n[INSERT] ts={sys_ingested_at}  user={username}  fields={list(record.keys())}")

        # Load 3NF dimension-table metadata once per call
        dim_tables = metadata_manager.get_3nf_dimension_tables()

        # Build a set of all fields that live exclusively in a dimension table
        # (i.e. the dependent / non-PK fields).  These must NOT appear in records.
        dim_dependent_fields: set[str] = set()
        for dim_info in dim_tables.values():
            dim_dependent_fields.update(dim_info["dependent_fields"])

        print(f"  [INSERT] dim_tables={list(dim_tables.keys())}  dim_dependent_fields={dim_dependent_fields}")

        try:
            with self.sql_engine.connect() as conn:

                # ── Step 1: dimension table inserts (3NF) ─────────────────────
                for dim_table_name, dim_info in dim_tables.items():
                    det = dim_info["determinant"]
                    if det not in record or record[det] is None:
                        print(f"  [INSERT] dim '{dim_table_name}': det='{det}' missing/null in record — skipped")
                        continue                 # FK value missing; skip this dim

                    dim_row: dict[str, Any] = {}
                    for f in dim_info["all_fields"]:
                        if f in record and record[f] is not None:
                            dim_row[f] = self._value_to_sql(record[f])

                    if len(dim_row) < 2:
                        # Only the PK itself is present; nothing useful to store
                        print(f"  [INSERT] dim '{dim_table_name}': only PK present, nothing to store — skipped")
                        continue

                    cols_str     = ", ".join(f'"{k}"' for k in dim_row)
                    safe_dim_row = {self._safe_key(k): v for k, v in dim_row.items()}
                    placeholders = ", ".join(f":{self._safe_key(k)}" for k in dim_row)
                    print(f"  [INSERT] dim '{dim_table_name}': INSERT OR IGNORE fields={list(dim_row.keys())}")
                    # INSERT OR IGNORE: the same student_id may arrive thousands
                    # of times; only the first write matters for the dim table.
                    conn.execute(
                        text(f'INSERT OR IGNORE INTO "{dim_table_name}" ({cols_str}) VALUES ({placeholders})'),
                        safe_dim_row,
                    )

                # ── Step 2: main 'records' table ──────────────────────────────
                if "records" in sql_tables:
                    main_info = sql_tables["records"]
                    row: dict[str, Any] = {}

                    for col in main_info["columns"]:
                        if col in dim_dependent_fields:
                            # This field now lives in a dimension table; skip it.
                            continue

                        if col in record:
                            row[col] = self._value_to_sql(record[col])
                        elif col in flattened_objects:
                            # Inline-flattened nested object (e.g. address.city)
                            obj = record.get(col)
                            if isinstance(obj, dict):
                                for f_col in flattened_objects[col]:
                                    sub_key = f_col.split(".")[-1]
                                    row[f_col] = self._value_to_sql(obj.get(sub_key))
                        # Dot-notation columns from flattened objects
                        elif "." in col:
                            parent_field = col.split(".")[0]
                            sub_key      = col.split(".")[-1]
                            obj = record.get(parent_field)
                            if isinstance(obj, dict):
                                row[col] = self._value_to_sql(obj.get(sub_key))

                    if row:
                        cols_str     = ", ".join(f'"{k}"' for k in row)
                        safe_row     = {self._safe_key(k): v for k, v in row.items()}
                        placeholders = ", ".join(f":{self._safe_key(k)}" for k in row)
                        print(f"  [INSERT] records: INSERT OR REPLACE fields={list(row.keys())}")
                        conn.execute(
                            text(f'INSERT OR REPLACE INTO "records" ({cols_str}) VALUES ({placeholders})'),
                            safe_row,
                        )
                    else:
                        print(f"  [INSERT] records: row is EMPTY — nothing written! schema_cols={list(main_info['columns'].keys())} dim_dependent={dim_dependent_fields}")
                else:
                    print("  [INSERT] records: 'records' not in sql_tables metadata — skipped")

                # ── Step 3: child tables (2NF repeating groups / nested objects)
                for table_name, info in sql_tables.items():
                    if table_name in ("records",) or table_name in dim_tables:
                        continue

                    field_name = table_name
                    if field_name not in record or not isinstance(record[field_name], list):
                        continue

                    item_count = 0
                    for item in record[field_name]:
                        if not isinstance(item, dict):
                            continue
                        child_row: dict[str, Any] = {
                            config.JOIN_KEY:           sys_ingested_at,
                            config.SECONDARY_JOIN_KEY: username,
                        }
                        for col in info["columns"]:
                            if col in ("_row_id", config.JOIN_KEY, config.SECONDARY_JOIN_KEY):
                                continue
                            if col in item:
                                child_row[col] = self._value_to_sql(item[col])
                            elif col in record:
                                child_row[col] = self._value_to_sql(record[col])

                        cols_str      = ", ".join(f'"{k}"' for k in child_row)
                        safe_child    = {self._safe_key(k): v for k, v in child_row.items()}
                        placeholders  = ", ".join(f":{self._safe_key(k)}" for k in child_row)
                        conn.execute(
                            text(f'INSERT INTO "{table_name}" ({cols_str}) VALUES ({placeholders})'),
                            safe_child,
                        )
                        item_count += 1
                    print(f"  [INSERT] child '{table_name}': inserted {item_count} rows")

                print(f"  [INSERT] committing SQL transaction")
                conn.commit()
                print(f"  [INSERT] SQL commit OK")

            # ── Step 4: MongoDB ────────────────────────────────────────────────
            if self.mongo_available:
                for coll_name, info in mongo_collections.items():
                    if info["strategy"] == "embed":
                        doc = {
                            config.JOIN_KEY:           sys_ingested_at,
                            config.SECONDARY_JOIN_KEY: username,
                        }
                        for field in info.get("embedded_fields", []):
                            if field in record:
                                doc[field] = record[field]
                        print(f"  [INSERT] mongo '{coll_name}': embed insert fields={list(doc.keys())}")
                        self.mongo_db[coll_name].insert_one(doc)

                    elif info["strategy"] == "reference":
                        if coll_name in record and isinstance(record[coll_name], list):
                            docs = []
                            for item in record[coll_name]:
                                doc = {
                                    config.JOIN_KEY:           sys_ingested_at,
                                    config.SECONDARY_JOIN_KEY: username,
                                }
                                if isinstance(item, dict):
                                    doc.update(item)
                                docs.append(doc)
                            if docs:
                                print(f"  [INSERT] mongo '{coll_name}': reference insert {len(docs)} docs")
                                self.mongo_db[coll_name].insert_many(docs)
                # ── Fallback: mongo-classified fields not covered by any collection ──
                # This happens when mongo_strategy has not run yet (e.g. no schema
                # registered). Any field whose placement says backend=mongo but which
                # wasn't handled by a named collection above goes to a default
                # "main_documents" collection so nothing is silently dropped.
                covered_fields: set[str] = set()
                for info in mongo_collections.values():
                    covered_fields.update(info.get("embedded_fields", []))
                    covered_fields.add(info.get("collection", ""))

                fallback_doc: dict[str, Any] = {
                    config.JOIN_KEY:           sys_ingested_at,
                    config.SECONDARY_JOIN_KEY: username,
                }
                for k, v in record.items():
                    p = field_placement.get(k, {})
                    if p.get("backend") == "mongo" and k not in covered_fields:
                        fallback_doc[k] = v

                if len(fallback_doc) > 2:   # more than just the two join keys
                    print(f"  [INSERT] mongo 'main_documents' (fallback): inserting fields={list(fallback_doc.keys())}")
                    self.mongo_db["main_documents"].insert_one(fallback_doc)

            elif not self.mongo_available:
                print(f"  [INSERT] MongoDB unavailable — SQL-only")

        except Exception as e:
            msg = f"Insert error for {sys_ingested_at}: {e}"
            print(msg, file=sys.stderr)
            logger.error(msg)
            raise  # re-raise so callers (main.py) can count the error correctly

    # ──────────────────────────────────────────────────────────────────────────

    def insert_pending_field_value(
        self, sys_ingested_at: str, field_name: str, value: Any, decision: dict
    ) -> None:
        """
        Update a previously undecided (buffered) field value once it has been
        classified.  Routes to the correct SQL table (including 3NF dim tables)
        or MongoDB collection.
        """
        backend = decision.get("backend")
        try:
            if backend == "sql":
                # A 3NF decomposition may have moved this field to a dim table.
                # Prefer the placement table; fall back to 'records'.
                table = decision.get("table", "records")
                det   = None

                # For dimension-table dependent fields, we need the FK value to
                # locate the correct row.
                dim_tables = metadata_manager.get_3nf_dimension_tables()
                for dim_name, dim_info in dim_tables.items():
                    if field_name in dim_info["dependent_fields"]:
                        table = dim_name
                        det   = dim_info["determinant"]
                        break

                with self.sql_engine.connect() as conn:
                    if det:
                        # We can't easily look up the FK from just the timestamp,
                        # so find the determinant value via the records row first.
                        row = conn.execute(
                            text(f'SELECT "{det}" FROM "records" WHERE "{config.JOIN_KEY}" = :ts'),
                            {"ts": sys_ingested_at},
                        ).mappings().first()
                        if row and row[det] is not None:
                            conn.execute(
                                text(f'UPDATE "{table}" SET "{field_name}" = :val WHERE "{det}" = :pk'),
                                {"val": self._value_to_sql(value), "pk": row[det]},
                            )
                    else:
                        conn.execute(
                            text(f'UPDATE "{table}" SET "{field_name}" = :val WHERE "{config.JOIN_KEY}" = :ts'),
                            {"val": self._value_to_sql(value), "ts": sys_ingested_at},
                        )
                    conn.commit()

            elif backend == "mongo" and self.mongo_available:
                coll = decision.get("collection", "main_documents")
                self.mongo_db[coll].update_one(
                    {config.JOIN_KEY: sys_ingested_at},
                    {"$set": {field_name: value}},
                )
        except Exception as e:
            print(
                f"  Pending update error for {sys_ingested_at}.{field_name}: {e}",
                file=sys.stderr,
            )

    # ──────────────────────────────────────────────────────────────────────────
    # READ
    # ──────────────────────────────────────────────────────────────────────────

    def execute_read(
        self,
        operation: dict,
        sql_tables: dict,
        mongo_collections: dict,
        field_placement: dict,
        flattened_objects: dict,
    ) -> list[dict]:
        """
        Reads and merges data from SQL (including 3NF dimension tables) and MongoDB.

        SQL read strategy
        -----------------
        1. Query the main 'records' table (with optional WHERE filters).
        2. For each 3NF dimension table, fetch rows matching the FK values found
           in step 1 and attach the dependent fields to each result record.
           (Many-to-one JOIN: many records rows → one dim row per FK value.)
        3. For each 2NF child table, fetch rows matching the JOIN_KEY values and
           append them as lists on the result records.
           (One-to-many JOIN: one records row → many child rows.)
        4. Reconstruct any flattened nested objects.
        """
        filters          = operation.get("filters", {})
        requested_fields = operation.get("fields")

        print(f"\n[READ] filters={filters}  requested_fields={requested_fields}")

        dim_tables = metadata_manager.get_3nf_dimension_tables()

        # Build set of all fields that live in dimension tables (for filter routing)
        dim_field_to_table: dict[str, tuple[str, str]] = {}  # field -> (dim_table, det)
        dim_dependent_fields: set[str] = set()
        for dim_name, dim_info in dim_tables.items():
            det = dim_info["determinant"]
            dim_dependent_fields.update(dim_info["dependent_fields"])
            for f in dim_info["all_fields"]:
                dim_field_to_table[f] = (dim_name, det)

        print(f"  [READ] dim_tables={list(dim_tables.keys())}  dim_dependent_fields={dim_dependent_fields}")

        sql_results: dict[str, dict] = {}

        try:
            with self.sql_engine.connect() as conn:

                # Query sqlite_master on THIS connection for a consistent view of
                # which tables exist.  inspect(self.sql_engine) opens a second
                # connection whose deferred transaction may snapshot sqlite_master
                # BEFORE our tables were committed, causing "records" to appear
                # missing and the entire read to silently return nothing.
                existing_tables: set[str] = {
                    row[0] for row in conn.execute(
                        text("SELECT name FROM sqlite_master WHERE type='table'")
                    ).all()
                }
                print(f"  [READ] existing_tables={existing_tables}")

                # ── Step 1: main 'records' query ──────────────────────────────
                if "records" not in existing_tables:
                    print("  [READ] 'records' table does not exist yet — returning empty")
                    pass
                else:
                    where_clauses: list[str] = []
                    params: dict[str, Any]  = {}

                    for k, v in filters.items():
                        p = field_placement.get(k, {})
                        # Only filter on columns that are actually in 'records'
                        if p.get("backend") == "sql" and p.get("table", "records") == "records":
                            if k not in dim_dependent_fields:
                                safe_k = self._safe_key(k)
                                where_clauses.append(f'"{k}" = :{safe_k}')
                                params[safe_k] = v
                            else:
                                print(f"  [READ] filter '{k}' lives in a dim table — skipped from records WHERE")
                        else:
                            print(f"  [READ] filter '{k}' not routed to records (placement={field_placement.get(k)}) — skipped")

                    where_str = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
                    sql_query = f'SELECT * FROM "records" {where_str}'
                    print(f"  [READ] step1 query: {sql_query}  params={params}")
                    rows = conn.execute(text(sql_query), params).mappings().all()
                    print(f"  [READ] step1 result: {len(rows)} rows from 'records'")

                    for row in rows:
                        res = dict(row)
                        # Reconstruct flattened nested objects
                        for orig_field, flat_cols in flattened_objects.items():
                            obj: dict[str, Any] = {}
                            for col in flat_cols:
                                if col in res:
                                    sub_key = col.split(".")[-1]
                                    obj[sub_key] = res.pop(col)
                            if obj:
                                res[orig_field] = obj
                        ts = res.get(config.JOIN_KEY)
                        if ts is None:
                            print(f"  [READ] WARNING: row missing JOIN_KEY '{config.JOIN_KEY}', keys={list(res.keys())} — skipped")
                            logger.warning("execute_read: row missing JOIN_KEY, skipping: %s", list(res.keys()))
                            continue
                        sql_results[ts] = res

                    print(f"  [READ] sql_results after step1: {len(sql_results)} records")

                    if sql_results:
                        # ── Step 2: 3NF dimension-table lookups (many-to-one) ──
                        for dim_name, dim_info in dim_tables.items():
                            if dim_name not in existing_tables:
                                print(f"  [READ] step2 dim '{dim_name}': table doesn't exist — skipped")
                                continue          # dim table not yet created

                            det        = dim_info["determinant"]
                            dep_fields = dim_info["dependent_fields"]

                            # Collect unique FK values from the fetched records
                            fk_values: list[Any] = list({
                                res[det]
                                for res in sql_results.values()
                                if det in res and res[det] is not None
                            })
                            if not fk_values:
                                print(f"  [READ] step2 dim '{dim_name}': no FK values for det='{det}' in results — skipped")
                                continue

                            det_params   = {f"det{i}": v for i, v in enumerate(fk_values)}
                            placeholders = ", ".join(f":det{i}" for i in range(len(fk_values)))
                            dim_rows = conn.execute(
                                text(f'SELECT * FROM "{dim_name}" WHERE "{det}" IN ({placeholders})'),
                                det_params,
                            ).mappings().all()
                            print(f"  [READ] step2 dim '{dim_name}': {len(dim_rows)} dim rows matched for {len(fk_values)} FK values, attaching fields={dep_fields}")

                            # Build FK-value → dim-row lookup
                            det_to_dim: dict[Any, dict] = {
                                str(r[det]): dict(r) for r in dim_rows
                            }

                            # Attach dependent fields to each records row
                            for res in sql_results.values():
                                fk_val = res.get(det)
                                if fk_val is None:
                                    continue
                                dim_row = det_to_dim.get(str(fk_val), {})
                                for f in dep_fields:
                                    if f in dim_row:
                                        res[f] = dim_row[f]

                        # ── Step 3: 2NF child table lookups (one-to-many) ──────
                        for table_name in sql_tables:
                            if table_name in ("records",) or table_name in dim_tables:
                                continue
                            if table_name not in existing_tables:
                                print(f"  [READ] step3 child '{table_name}': table doesn't exist — skipped")
                                continue          # child table not yet created

                            ts_list      = list(sql_results.keys())
                            ts_params    = {f"ts{i}": ts for i, ts in enumerate(ts_list)}
                            placeholders = ", ".join(f":ts{i}" for i in range(len(ts_list)))
                            child_rows   = conn.execute(
                                text(f'SELECT * FROM "{table_name}" WHERE "{config.JOIN_KEY}" IN ({placeholders})'),
                                ts_params,
                            ).mappings().all()
                            print(f"  [READ] step3 child '{table_name}': {len(child_rows)} rows fetched")

                            for crow in child_rows:
                                ts = crow[config.JOIN_KEY]
                                if ts not in sql_results:
                                    continue
                                sql_results[ts].setdefault(table_name, [])
                                clean = dict(crow)
                                clean.pop("_row_id", None)
                                sql_results[ts][table_name].append(clean)
                    else:
                        print("  [READ] sql_results empty after step1 — skipping dim/child lookups")

        except Exception as e:
            print(f"  [READ] SQL error: {e}", file=sys.stderr)
            logger.error(f"SQL Read error: {e}")

        # ── MongoDB reads ──────────────────────────────────────────────────────
        mongo_results: dict[str, dict] = {}
        if self.mongo_available:
            try:
                mongo_filters: dict[str, Any] = {}
                for k, v in filters.items():
                    p = field_placement.get(k, {})
                    if p.get("backend") == "mongo":
                        mongo_filters[k] = v
                if config.SECONDARY_JOIN_KEY in filters:
                    mongo_filters[config.SECONDARY_JOIN_KEY] = filters[config.SECONDARY_JOIN_KEY]
                if config.JOIN_KEY in filters:
                    mongo_filters[config.JOIN_KEY] = filters[config.JOIN_KEY]

                for coll_name, info in mongo_collections.items():
                    docs = list(self.mongo_db[coll_name].find(mongo_filters, {"_id": 0}))
                    print(f"  [READ] mongo '{coll_name}': {len(docs)} docs found (strategy={info['strategy']})")
                    for d in docs:
                        ts = d.get(config.JOIN_KEY)
                        if not ts:
                            continue
                        mongo_results.setdefault(ts, {})
                        if info["strategy"] == "embed":
                            mongo_results[ts].update(d)
                        else:
                            mongo_results[ts].setdefault(coll_name, []).append(d)

                # Also read from the fallback "main_documents" collection used when
                # mongo_strategy has not run yet (no schema registered).
                if "main_documents" not in mongo_collections:
                    fallback_docs = list(self.mongo_db["main_documents"].find(mongo_filters, {"_id": 0}))
                    print(f"  [READ] mongo 'main_documents' (fallback): {len(fallback_docs)} docs found")
                    for d in fallback_docs:
                        ts = d.get(config.JOIN_KEY)
                        if not ts:
                            continue
                        mongo_results.setdefault(ts, {})
                        mongo_results[ts].update(d)
            except Exception as e:
                logger.error(f"Mongo Read error: {e}")
        else:
            print("  [READ] MongoDB unavailable — SQL-only")

        # ── Merge SQL + Mongo ──────────────────────────────────────────────────
        all_timestamps = set(sql_results.keys()) | set(mongo_results.keys())
        print(f"  [READ] merging: {len(sql_results)} SQL records + {len(mongo_results)} Mongo records = {len(all_timestamps)} total")
        merged: list[dict] = []
        for ts in all_timestamps:
            record: dict[str, Any] = {}
            record.update(sql_results.get(ts, {}))
            for k, v in mongo_results.get(ts, {}).items():
                if k not in record or record[k] is None:
                    record[k] = v
            if requested_fields:
                record = {k: v for k, v in record.items() if k in requested_fields}
            merged.append(record)

        print(f"  [READ] returning {len(merged)} merged records")
        return merged

    # ──────────────────────────────────────────────────────────────────────────
    # UPDATE  (was missing entirely in the 2NF version)
    # ──────────────────────────────────────────────────────────────────────────

    def execute_update(
        self,
        operation: dict,
        sql_tables: dict,
        mongo_collections: dict,
        field_placement: dict,
        flattened_objects: dict,
        schema: dict,
    ) -> dict:
        """
        Apply a SET of field updates to records matching the given filters.

        3NF-aware routing:
          • Fields that live in a 3NF dimension table are updated there,
            identified via the FK value in the matching 'records' row.
          • Fields that live in 'records' (scalar columns or FK columns) are
            updated directly.
          • MongoDB fields are updated via update_many.

        Returns {"updated_sql_rows": int, "updated_mongo_docs": int}.
        """
        filters = operation.get("filters", {})
        updates = operation.get("set", {})

        print(f"\n[UPDATE] filters={filters}  set={updates}")

        if not filters or not updates:
            print("  [UPDATE] no filters or no updates — nothing to do")
            return {"updated_sql_rows": 0, "updated_mongo_docs": 0}

        dim_tables = metadata_manager.get_3nf_dimension_tables()

        # Split update fields by destination
        records_updates: dict[str, Any]              = {}
        dim_updates: dict[str, dict[str, Any]]       = {}   # dim_table -> {field: val}
        child_updates: dict[str, dict[str, Any]]     = {}   # child_table -> {field: val}
        mongo_updates: dict[str, Any]                = {}

        for field, val in updates.items():
            p = field_placement.get(field, {})
            backend = p.get("backend")
            table   = p.get("table", "records")

            if backend == "sql":
                # Check if this field is in a 3NF dimension table
                in_dim = False
                for dim_name, dim_info in dim_tables.items():
                    if field in dim_info["dependent_fields"]:
                        dim_updates.setdefault(dim_name, {})[field] = val
                        in_dim = True
                        print(f"  [UPDATE] field '{field}' → dim table '{dim_name}'")
                        break
                if not in_dim:
                    if table == "records":
                        records_updates[field] = val
                        print(f"  [UPDATE] field '{field}' → records table")
                    else:
                        child_updates.setdefault(table, {})[field] = val
                        print(f"  [UPDATE] field '{field}' → child table '{table}'")
            elif backend == "mongo":
                mongo_updates[field] = val
                print(f"  [UPDATE] field '{field}' → mongo")
            else:
                print(f"  [UPDATE] field '{field}' has no placement (backend={backend}) — skipped")

        sql_count   = 0
        mongo_count = 0

        try:
            with self.sql_engine.connect() as conn:

                # ── Build WHERE for 'records' filters ─────────────────────────
                rec_where: list[str] = []
                rec_params: dict[str, Any] = {}
                for k, v in filters.items():
                    p = field_placement.get(k, {})
                    if p.get("backend") == "sql" and p.get("table", "records") == "records":
                        safe_k = self._safe_key(k)
                        rec_where.append(f'"{k}" = :filter_{safe_k}')
                        rec_params[f"filter_{safe_k}"] = v

                where_str = f"WHERE {' AND '.join(rec_where)}" if rec_where else ""
                print(f"  [UPDATE] WHERE clause: {where_str}  params={rec_params}")

                # ── Update 'records' scalar columns ───────────────────────────
                if records_updates:
                    set_clauses = [f'"{k}" = :set_{self._safe_key(k)}' for k in records_updates]
                    params = {**rec_params,
                              **{f"set_{self._safe_key(k)}": self._value_to_sql(v)
                                 for k, v in records_updates.items()}}
                    print(f"  [UPDATE] records SET {set_clauses}")
                    res = conn.execute(
                        text(f'UPDATE "records" SET {", ".join(set_clauses)} {where_str}'),
                        params,
                    )
                    sql_count += res.rowcount
                    print(f"  [UPDATE] records: {res.rowcount} rows updated")

                # ── Update 3NF dimension-table rows ───────────────────────────
                if dim_updates and rec_where:
                    # Fetch FK values for matching records rows
                    for dim_name, field_vals in dim_updates.items():
                        det = dim_tables[dim_name]["determinant"]
                        fk_rows = conn.execute(
                            text(f'SELECT DISTINCT "{det}" FROM "records" {where_str}'),
                            rec_params,
                        ).mappings().all()
                        fk_values = [r[det] for r in fk_rows if r[det] is not None]
                        print(f"  [UPDATE] dim '{dim_name}': found {len(fk_values)} FK values for det='{det}'")
                        if not fk_values:
                            continue

                        set_clauses = [f'"{k}" = :set_{self._safe_key(k)}' for k in field_vals]
                        fk_placeholders = ", ".join(f":fk{i}" for i in range(len(fk_values)))
                        params = {
                            **{f"set_{self._safe_key(k)}": self._value_to_sql(v) for k, v in field_vals.items()},
                            **{f"fk{i}": v for i, v in enumerate(fk_values)},
                        }
                        res = conn.execute(
                            text(
                                f'UPDATE "{dim_name}" '
                                f'SET {", ".join(set_clauses)} '
                                f'WHERE "{det}" IN ({fk_placeholders})'
                            ),
                            params,
                        )
                        sql_count += res.rowcount
                        print(f"  [UPDATE] dim '{dim_name}': {res.rowcount} rows updated")

                # ── Update 2NF child table rows ────────────────────────────────
                if child_updates and rec_where:
                    ts_rows = conn.execute(
                        text(f'SELECT "{config.JOIN_KEY}" FROM "records" {where_str}'),
                        rec_params,
                    ).mappings().all()
                    ts_values = [r[config.JOIN_KEY] for r in ts_rows]
                    print(f"  [UPDATE] child tables: found {len(ts_values)} matching timestamps")
                    if ts_values:
                        ts_placeholders = ", ".join(f":ts{i}" for i in range(len(ts_values)))
                        ts_params = {f"ts{i}": v for i, v in enumerate(ts_values)}
                        for child_table, field_vals in child_updates.items():
                            set_clauses = [f'"{k}" = :set_{self._safe_key(k)}' for k in field_vals]
                            params = {
                                **{f"set_{self._safe_key(k)}": self._value_to_sql(v) for k, v in field_vals.items()},
                                **ts_params,
                            }
                            res = conn.execute(
                                text(
                                    f'UPDATE "{child_table}" '
                                    f'SET {", ".join(set_clauses)} '
                                    f'WHERE "{config.JOIN_KEY}" IN ({ts_placeholders})'
                                ),
                                params,
                            )
                            sql_count += res.rowcount
                            print(f"  [UPDATE] child '{child_table}': {res.rowcount} rows updated")

                conn.commit()
                print(f"  [UPDATE] SQL commit OK — total rows updated: {sql_count}")

        except Exception as e:
            print(f"  [UPDATE] SQL error: {e}", file=sys.stderr)
            logger.error(f"SQL Update error: {e}")

        # ── MongoDB updates ────────────────────────────────────────────────────
        if self.mongo_available and mongo_updates:
            try:
                mongo_filter: dict[str, Any] = {}
                for k, v in filters.items():
                    mongo_filter[k] = v
                for coll_name in mongo_collections:
                    res = self.mongo_db[coll_name].update_many(
                        mongo_filter, {"$set": mongo_updates}
                    )
                    mongo_count += res.modified_count
                    print(f"  [UPDATE] mongo '{coll_name}': {res.modified_count} docs updated")
            except Exception as e:
                logger.error(f"Mongo Update error: {e}")

        print(f"  [UPDATE] done — sql_rows={sql_count}  mongo_docs={mongo_count}")
        return {"updated_sql_rows": sql_count, "updated_mongo_docs": mongo_count}

    # ──────────────────────────────────────────────────────────────────────────
    # DELETE
    # ──────────────────────────────────────────────────────────────────────────

    def execute_delete(
        self,
        operation: dict,
        sql_tables: dict,
        mongo_collections: dict,
    ) -> dict:
        """
        Delete records matching the given filters from all backends.

        Deletes cascade through child tables and dimension tables via JOIN_KEY
        and FK respectively.  For dimension tables we do NOT delete the dim row
        unless no other records reference that FK value (to preserve referential
        integrity), so dimension rows are left in place — this is safe because
        they carry only descriptive attributes, not transactional data.
        """
        filters = operation.get("filters", {})

        print(f"\n[DELETE] filters={filters}")

        if not filters:
            print("  [DELETE] no filters provided — refusing to delete everything")
            return {"deleted_sql_rows": 0, "deleted_mongo_docs": 0}

        read_op = {"filters": filters, "fields": [config.JOIN_KEY]}
        records = self.execute_read(
            read_op, sql_tables, mongo_collections,
            metadata_manager.get_field_placement(),
            metadata_manager.get_flattened_objects(),
        )
        timestamps = [r[config.JOIN_KEY] for r in records if config.JOIN_KEY in r]
        print(f"  [DELETE] found {len(timestamps)} matching records to delete")
        if not timestamps:
            return {"deleted_sql_rows": 0, "deleted_mongo_docs": 0}

        dim_tables = metadata_manager.get_3nf_dimension_tables()
        sql_count  = 0

        try:
            with self.sql_engine.connect() as conn:
                ts_placeholders = ", ".join(f":ts{i}" for i in range(len(timestamps)))
                ts_params       = {f"ts{i}": ts for i, ts in enumerate(timestamps)}

                # Delete from 'records' and all child tables (joined via JOIN_KEY)
                for table_name in sql_tables:
                    if table_name in dim_tables:
                        print(f"  [DELETE] dim table '{table_name}' preserved (referential integrity)")
                        continue    # dimension rows are intentionally preserved
                    res = conn.execute(
                        text(f'DELETE FROM "{table_name}" WHERE "{config.JOIN_KEY}" IN ({ts_placeholders})'),
                        ts_params,
                    )
                    print(f"  [DELETE] table '{table_name}': {res.rowcount} rows deleted")
                    if table_name == "records":
                        sql_count += res.rowcount

                conn.commit()
                print(f"  [DELETE] SQL commit OK")
        except Exception as e:
            print(f"  [DELETE] SQL error: {e}", file=sys.stderr)
            logger.error(f"SQL Delete error: {e}")

        mongo_count = 0
        if self.mongo_available:
            try:
                for coll_name in mongo_collections:
                    res = self.mongo_db[coll_name].delete_many(
                        {config.JOIN_KEY: {"$in": timestamps}}
                    )
                    mongo_count += res.deleted_count
                    print(f"  [DELETE] mongo '{coll_name}': {res.deleted_count} docs deleted")
            except Exception as e:
                logger.error(f"Mongo Delete error: {e}")

        print(f"  [DELETE] done — sql_rows={sql_count}  mongo_docs={mongo_count}")
        return {"deleted_sql_rows": sql_count, "deleted_mongo_docs": mongo_count}

    # ──────────────────────────────────────────────────────────────────────────
    # RESET
    # ──────────────────────────────────────────────────────────────────────────

    def reset_database(self, sql_tables: dict, mongo_collections: dict) -> None:
        """Drop all SQL tables and MongoDB collections, then dispose the engine."""
        print(f"\n[RESET] Dropping all SQL tables: {list(sql_tables.keys())}")
        with self.sql_engine.connect() as conn:
            for table_name in sql_tables:
                conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
                print(f"  [RESET] dropped table '{table_name}'")
            # Also drop any dimension tables that may exist in the DB but not in
            # the current sql_tables snapshot (e.g. from a previous schema run)
            dim_tables = metadata_manager.get_3nf_dimension_tables()
            for dim_name in dim_tables:
                if dim_name not in sql_tables:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{dim_name}"'))
                    print(f"  [RESET] dropped orphaned dim table '{dim_name}'")
            conn.commit()
        self.sql_engine.dispose()
        print("  [RESET] SQL engine disposed")

        if self.mongo_available:
            for coll_name in mongo_collections:
                self.mongo_db[coll_name].drop()
                print(f"  [RESET] dropped mongo collection '{coll_name}'")
