import json
import logging
import sys
from typing import Any
from sqlalchemy import create_engine, text, inspect
from pymongo import MongoClient
import pymongo.errors
import hybrid_framework.config as config
import hybrid_framework.metadata_manager as metadata_manager

# Configure logging to stderr for warnings
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

class CRUDManager:
    def __init__(self):
        self.sql_engine = create_engine(config.SQL_URL)
        try:
            self.mongo_client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=2000)
            self.mongo_db = self.mongo_client[config.MONGO_DB]
            # Test connection
            self.mongo_client.server_info()
            self.mongo_available = True
        except (pymongo.errors.ConnectionFailure, pymongo.errors.ServerSelectionTimeoutError):
            logger.warning("MongoDB unavailable. SQL-only mode.")
            self.mongo_available = False

    def ensure_all_tables(self, sql_tables: dict) -> None:
        """Create or update SQL tables based on metadata."""
        with self.sql_engine.connect() as conn:
            inspector = inspect(self.sql_engine)
            for table_name, info in sql_tables.items():
                if not inspector.has_table(table_name):
                    # Create table
                    cols = []
                    pk = info.get("primary_key")
                    for col_name, col_info in info["columns"].items():
                        col_def = f'"{col_name}" {col_info["sql_type"]}'
                        if col_name == pk:
                            col_def += " PRIMARY KEY"
                        if col_info.get("not_null"):
                            col_def += " NOT NULL"
                        if col_info.get("unique"):
                            col_def += " UNIQUE"
                        cols.append(col_def)
                    
                    # Add _row_id if no PK
                    if not pk:
                        cols.append("_row_id INTEGER PRIMARY KEY AUTOINCREMENT")
                        
                    query = f'CREATE TABLE "{table_name}" ({", ".join(cols)})'
                    conn.execute(text(query))
                    conn.commit()
                else:
                    # Check for missing columns
                    existing_cols = [c["name"] for c in inspector.get_columns(table_name)]
                    for col_name, col_info in info["columns"].items():
                        if col_name not in existing_cols:
                            query = f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_info["sql_type"]}'
                            conn.execute(text(query))
                            conn.commit()

    def _value_to_sql(self, value: Any) -> Any:
        if value is None: return None
        if isinstance(value, (dict, list)): return json.dumps(value)
        if isinstance(value, bool): return 1 if value else 0
        return value

    def insert_record(self, record: dict, sql_tables: dict, mongo_collections: dict, 
                      field_placement: dict, flattened_objects: dict) -> None:
        """Splits one normalized record across all backends."""
        sys_ingested_at = record.get(config.JOIN_KEY)
        username = record.get(config.SECONDARY_JOIN_KEY)
        
        try:
            # 1. SQL Insertion
            with self.sql_engine.connect() as conn:
                for table_name, info in sql_tables.items():
                    row = {}
                    is_main = (table_name == "records")
                    
                    if is_main:
                        # Build main row
                        for col in info["columns"]:
                            if col in record:
                                row[col] = self._value_to_sql(record[col])
                            elif col in flattened_objects:
                                # Flattened object logic
                                obj = record.get(col)
                                if isinstance(obj, dict):
                                    for f_col in flattened_objects[col]:
                                        sub_key = f_col.split(".")[-1]
                                        row[f_col] = self._value_to_sql(obj.get(sub_key))
                        
                        if row:
                            cols_str = ", ".join([f'"{k}"' for k in row.keys()])
                            placeholders = ", ".join([f":{k}" for k in row.keys()])
                            query = f'INSERT OR REPLACE INTO "{table_name}" ({cols_str}) VALUES ({placeholders})'
                            conn.execute(text(query), row)
                    else:
                        # Child table
                        # Map table name to record field (handle normalization engine naming)
                        field_name = table_name 
                        if field_name in record and isinstance(record[field_name], list):
                            for item in record[field_name]:
                                child_row = {config.JOIN_KEY: sys_ingested_at, config.SECONDARY_JOIN_KEY: username}
                                if isinstance(item, dict):
                                    for col in info["columns"]:
                                        if col in item:
                                            child_row[col] = self._value_to_sql(item[col])
                                        elif col in record: # e.g. parent PK
                                            child_row[col] = self._value_to_sql(record[col])
                                    
                                    cols_str = ", ".join([f'"{k}"' for k in child_row.keys()])
                                    placeholders = ", ".join([f":{k}" for k in child_row.keys()])
                                    query = f'INSERT INTO "{table_name}" ({cols_str}) VALUES ({placeholders})'
                                    conn.execute(text(query), child_row)
                conn.commit()

            # 2. MongoDB Insertion
            if self.mongo_available:
                for coll_name, info in mongo_collections.items():
                    if info["strategy"] == "embed":
                        doc = {config.JOIN_KEY: sys_ingested_at, config.SECONDARY_JOIN_KEY: username}
                        for field in info.get("embedded_fields", []):
                            if field in record:
                                doc[field] = record[field]
                        self.mongo_db[coll_name].insert_one(doc)
                    elif info["strategy"] == "reference":
                        if coll_name in record and isinstance(record[coll_name], list):
                            docs = []
                            for item in record[coll_name]:
                                doc = {config.JOIN_KEY: sys_ingested_at, config.SECONDARY_JOIN_KEY: username}
                                if isinstance(item, dict):
                                    doc.update(item)
                                docs.append(doc)
                            if docs:
                                self.mongo_db[coll_name].insert_many(docs)
        except Exception as e:
            print(f"  Insert error for {sys_ingested_at}: {e}", file=sys.stderr)

    def insert_pending_field_value(self, sys_ingested_at: str, field_name: str, value: Any, decision: dict) -> None:
        """Update previously undecided field values."""
        backend = decision.get("backend")
        try:
            if backend == "sql":
                table = decision.get("table", "records")
                with self.sql_engine.connect() as conn:
                    query = f'UPDATE "{table}" SET "{field_name}" = :val WHERE "{config.JOIN_KEY}" = :ts'
                    conn.execute(text(query), {"val": self._value_to_sql(value), "ts": sys_ingested_at})
                    conn.commit()
            elif backend == "mongo" and self.mongo_available:
                coll = decision.get("collection", "main_documents")
                self.mongo_db[coll].update_one(
                    {config.JOIN_KEY: sys_ingested_at},
                    {"$set": {field_name: value}}
                )
        except Exception as e:
            print(f"  Pending update error for {sys_ingested_at}.{field_name}: {e}", file=sys.stderr)

    def execute_read(self, operation: dict, sql_tables: dict, mongo_collections: dict, 
                     field_placement: dict, flattened_objects: dict) -> list[dict]:
        """Reads and merges data from SQL and MongoDB."""
        filters = operation.get("filters", {})
        requested_fields = operation.get("fields")
        
        sql_results = {} # sys_ingested_at -> dict
        try:
            with self.sql_engine.connect() as conn:
                where_clauses = []
                params = {}
                for k, v in filters.items():
                    placement = field_placement.get(k)
                    if placement and placement["backend"] == "sql" and placement.get("table") == "records":
                        where_clauses.append(f'"{k}" = :{k}')
                        params[k] = v
                
                where_str = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
                query = f'SELECT * FROM "records" {where_str}'
                rows = conn.execute(text(query), params).mappings().all()
                
                for row in rows:
                    res = dict(row)
                    for orig_field, flat_cols in flattened_objects.items():
                        obj = {}
                        for col in flat_cols:
                            if col in res:
                                sub_key = col.split(".")[-1]
                                obj[sub_key] = res.pop(col)
                        if obj:
                            res[orig_field] = obj
                    sql_results[res[config.JOIN_KEY]] = res
                    
                for table_name in sql_tables:
                    if table_name == "records": continue
                    if not sql_results: break
                    ts_params = {f"ts{i}": ts for i, ts in enumerate(sql_results.keys())}
                    query = f'SELECT * FROM "{table_name}" WHERE "{config.JOIN_KEY}" IN ({", ".join([f":ts{i}" for i in range(len(sql_results))])})'
                    
                    child_rows = conn.execute(text(query), ts_params).mappings().all()
                    for crow in child_rows:
                        ts = crow[config.JOIN_KEY]
                        if ts in sql_results:
                            field_name = table_name 
                            if field_name not in sql_results[ts]:
                                sql_results[ts][field_name] = []
                            
                            clean_crow = dict(crow)
                            clean_crow.pop("_row_id", None)
                            sql_results[ts][field_name].append(clean_crow)
        except Exception as e:
            logger.error(f"SQL Read error: {e}")

        mongo_results = {}
        if self.mongo_available:
            try:
                mongo_filters = {}
                for k, v in filters.items():
                    placement = field_placement.get(k)
                    if placement and placement["backend"] == "mongo":
                        mongo_filters[k] = v
                if config.SECONDARY_JOIN_KEY in filters: mongo_filters[config.SECONDARY_JOIN_KEY] = filters[config.SECONDARY_JOIN_KEY]
                if config.JOIN_KEY in filters: mongo_filters[config.JOIN_KEY] = filters[config.JOIN_KEY]

                for coll_name, info in mongo_collections.items():
                    docs = list(self.mongo_db[coll_name].find(mongo_filters, {"_id": 0}))
                    for d in docs:
                        ts = d.get(config.JOIN_KEY)
                        if not ts: continue
                        if ts not in mongo_results: mongo_results[ts] = {}
                        
                        if info["strategy"] == "embed":
                            mongo_results[ts].update(d)
                        else:
                            if coll_name not in mongo_results[ts]: mongo_results[ts][coll_name] = []
                            mongo_results[ts][coll_name].append(d)
            except Exception as e:
                logger.error(f"Mongo Read error: {e}")

        all_timestamps = set(sql_results.keys()) | set(mongo_results.keys())
        merged = []
        for ts in all_timestamps:
            record = {}
            record.update(sql_results.get(ts, {}))
            m_doc = mongo_results.get(ts, {})
            for k, v in m_doc.items():
                if k not in record or record[k] is None:
                    record[k] = v
            if requested_fields:
                record = {k: v for k, v in record.items() if k in requested_fields}
            merged.append(record)
        return merged

    def execute_delete(self, operation: dict, sql_tables: dict, mongo_collections: dict) -> dict:
        filters = operation.get("filters", {})
        if not filters: return {"deleted_sql_rows": 0, "deleted_mongo_docs": 0}
        
        read_op = {"filters": filters, "fields": [config.JOIN_KEY]}
        records = self.execute_read(read_op, sql_tables, mongo_collections, metadata_manager.get_field_placement(), metadata_manager.get_flattened_objects())
        timestamps = [r[config.JOIN_KEY] for r in records if config.JOIN_KEY in r]
        
        if not timestamps:
            return {"deleted_sql_rows": 0, "deleted_mongo_docs": 0}
            
        sql_count = 0
        try:
            with self.sql_engine.connect() as conn:
                for table_name in sql_tables:
                    query = f'DELETE FROM "{table_name}" WHERE "{config.JOIN_KEY}" IN ({", ".join([f":ts{i}" for i in range(len(timestamps))])})'
                    params = {f"ts{i}": ts for i, ts in enumerate(timestamps)}
                    res = conn.execute(text(query), params)
                    if table_name == "records":
                        sql_count += res.rowcount
                conn.commit()
        except Exception as e:
            logger.error(f"SQL Delete error: {e}")
            
        mongo_count = 0
        if self.mongo_available:
            try:
                for coll_name in mongo_collections:
                    res = self.mongo_db[coll_name].delete_many({config.JOIN_KEY: {"$in": timestamps}})
                    mongo_count += res.deleted_count
            except Exception as e:
                logger.error(f"Mongo Delete error: {e}")
                
        return {"deleted_sql_rows": sql_count, "deleted_mongo_docs": mongo_count}

    def reset_database(self, sql_tables: dict, mongo_collections: dict) -> None:
        """Drop all tables and collections, then dispose of the engine."""
        # 1. SQL Cleanup
        with self.sql_engine.connect() as conn:
            for table_name in sql_tables:
                conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
            conn.commit()
        self.sql_engine.dispose()

        # 2. MongoDB Cleanup
        if self.mongo_available:
            for coll_name in mongo_collections:
                self.mongo_db[coll_name].drop()
