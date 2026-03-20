import json
import os
from typing import Any
import hybrid_framework.config as config
import hybrid_framework.metadata_manager as metadata_manager
import hybrid_framework.analysis as analysis
import hybrid_framework.classification as classification
import hybrid_framework.crud as crud

class BufferManager:
    def __init__(self, crud_manager: crud.CRUDManager):
        self.crud_manager = crud_manager

    def load_buffer(self) -> dict:
        if not config.BUFFER_FILE.exists():
            return {"pending_fields": {}, "new_since_last_eval": 0}
        try:
            with open(config.BUFFER_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {"pending_fields": {}, "new_since_last_eval": 0}

    def save_buffer(self, data: dict) -> None:
        config.DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(config.BUFFER_FILE, "w") as f:
            json.dump(data, f, indent=2)

    def add_pending_fields(self, sys_ingested_at: str, undecided_fields: dict) -> None:
        buffer = self.load_buffer()
        if sys_ingested_at not in buffer["pending_fields"]:
            buffer["pending_fields"][sys_ingested_at] = {}
        buffer["pending_fields"][sys_ingested_at].update(undecided_fields)
        buffer["new_since_last_eval"] += 1
        self.save_buffer(buffer)
        
        if buffer["new_since_last_eval"] >= config.BUFFER_BATCH_SIZE:
            self.trigger_reevaluation()

    def trigger_reevaluation(self) -> dict:
        buffer = self.load_buffer()
        pending = buffer["pending_fields"]
        if not pending:
            return {"classified": [], "undecided": []}
            
        # Reconstruct mini-records
        mini_records = []
        for ts, fields in pending.items():
            rec = fields.copy()
            rec[config.JOIN_KEY] = ts
            mini_records.append(rec)
            
        # Analyze
        batch_stats = analysis.analyze_buffer(mini_records)
        prev_cumulative = metadata_manager.get_cumulative_stats()
        merged_raw = analysis.merge_cumulative_stats(prev_cumulative, batch_stats, len(mini_records))
        metadata_manager.save_cumulative_stats(merged_raw, merged_raw["total_records"])
        
        derived_stats = analysis.cumulative_raw_to_derived(merged_raw)
        decisions = classification.classify_fields(derived_stats)
        
        classified_count = 0
        for field, decision in decisions.items():
            if decision["backend"] in ("sql", "mongo"):
                self.commit_classified_field(field, decision)
                classified_count += 1
                
        # Reload buffer after commits
        buffer = self.load_buffer()
        buffer["new_since_last_eval"] = 0
        self.save_buffer(buffer)
        
        return {"classified_count": classified_count}

    def commit_classified_field(self, field_name: str, decision: dict) -> None:
        buffer = self.load_buffer()
        pending = buffer["pending_fields"]
        
        to_remove_ts = []
        for ts, fields in pending.items():
            if field_name in fields:
                val = fields.pop(field_name)
                # Commit to backend
                self.crud_manager.insert_pending_field_value(ts, field_name, val, decision)
            if not fields:
                to_remove_ts.append(ts)
                
        for ts in to_remove_ts:
            pending.pop(ts)
            
        self.save_buffer(buffer)
        metadata_manager.save_field_placement({field_name: decision})

    def force_flush(self) -> dict:
        buffer = self.load_buffer()
        pending = buffer["pending_fields"]
        all_fields = set()
        for fields in pending.values():
            all_fields.update(fields.keys())
            
        for field in all_fields:
            decision = {"backend": "mongo", "reason": "force_flush", "collection": "main_documents"}
            self.commit_classified_field(field, decision)
            
        return {"flushed_fields": list(all_fields)}

    def get_pending_field_names(self) -> set[str]:
        buffer = self.load_buffer()
        fields = set()
        for f in buffer["pending_fields"].values():
            fields.update(f.keys())
        return fields

    def get_buffer_stats(self) -> dict:
        buffer = self.load_buffer()
        return {
            "pending_record_count": len(buffer["pending_fields"]),
            "pending_field_names": list(self.get_pending_field_names()),
            "new_since_last_eval": buffer["new_since_last_eval"]
        }
