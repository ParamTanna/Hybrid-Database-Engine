from typing import Any
import hybrid_framework.config as config
import hybrid_framework.metadata_manager as metadata_manager
import hybrid_framework.schema_registry as schema_registry
import hybrid_framework.crud as crud
import hybrid_framework.ingest as ingest
import hybrid_framework.buffer_manager as buffer_manager

class QueryEngine:
    def __init__(self, crud_manager: crud.CRUDManager, buffer_mgr: buffer_manager.BufferManager):
        self.crud_manager = crud_manager
        self.buffer_manager = buffer_mgr

    def handle_query(self, operation_json: dict) -> dict:
        op_type = operation_json.get("operation")
        if op_type not in ("read", "insert", "delete", "update"):
            return {"status": "error", "message": f"Invalid operation: {op_type}"}
            
        sql_tables = metadata_manager.get_sql_tables()
        mongo_collections = metadata_manager.get_mongo_collections()
        field_placement = metadata_manager.get_field_placement()
        flattened_objects = metadata_manager.get_flattened_objects()
        schema = schema_registry.get_schema()
        
        try:
            if op_type == "read":
                data = self.crud_manager.execute_read(operation_json, sql_tables, mongo_collections, field_placement, flattened_objects)
                return {"status": "ok", "data": data}
                
            elif op_type == "insert":
                record = operation_json.get("record")
                if not record: return {"status": "error", "message": "Missing 'record'"}
                
                # Ingest pipeline
                processed = ingest.ingest_one(record)
                
                # Separate decided vs undecided
                decided_rec = {}
                undecided_fields = {}
                
                for k, v in processed.items():
                    placement = metadata_manager.get_placement_for_field(k)
                    if placement and placement["backend"] in ("sql", "mongo"):
                        decided_rec[k] = v
                    else:
                        undecided_fields[k] = v
                
                # Insert decided
                self.crud_manager.insert_record(decided_rec, sql_tables, mongo_collections, field_placement, flattened_objects)
                # Buffer undecided
                if undecided_fields:
                    ts = processed[config.JOIN_KEY]
                    self.buffer_manager.add_pending_fields(ts, undecided_fields)
                    
                return {"status": "ok", "summary": {"decided_fields": list(decided_rec.keys()), "buffered_fields": list(undecided_fields.keys())}}
                
            elif op_type == "delete":
                summary = self.crud_manager.execute_delete(operation_json, sql_tables, mongo_collections)
                return {"status": "ok", "summary": summary}
                
            elif op_type == "update":
                summary = self.crud_manager.execute_update(operation_json, sql_tables, mongo_collections, field_placement, flattened_objects, schema)
                return {"status": "ok", "summary": summary}
                
        except Exception as e:
            return {"status": "error", "message": str(e)}
            
        return {"status": "error", "message": "Unknown error"}
