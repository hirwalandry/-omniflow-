from __future__ import annotations
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..config import settings


class AuditLogger:
    def __init__(self, log_path: Optional[str] = None):
        self.log_path = log_path or os.path.join(settings.data_output_dir, "audit.log")
        self._events: List[Dict[str, Any]] = []

    def log(self, event_type: str, data: Dict[str, Any]) -> None:
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type,
            **data,
        }
        self._events.append(record)
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
        with open(self.log_path, "a") as f:
            f.write(json.dumps(record) + "\n")

    def log_ingestion(self, document_id: str, source: str, success: bool, error: Optional[str] = None) -> None:
        self.log("ingestion", {
            "document_id": document_id,
            "source": source,
            "success": success,
            "error": error,
        })

    def log_processing(self, document_id: str, stage: str, processor: str, status: str, duration_ms: Optional[float] = None, error: Optional[str] = None) -> None:
        self.log("processing", {
            "document_id": document_id,
            "stage": stage,
            "processor": processor,
            "status": status,
            "duration_ms": duration_ms,
            "error": error,
        })

    def log_storage(self, document_id: str, destination: str, success: bool, error: Optional[str] = None) -> None:
        self.log("storage", {
            "document_id": document_id,
            "destination": destination,
            "success": success,
            "error": error,
        })

    def log_error(self, document_id: str, stage: str, error: str, context: Optional[Dict[str, Any]] = None) -> None:
        self.log("error", {
            "document_id": document_id,
            "stage": stage,
            "error": error,
            "context": context or {},
        })

    def get_recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        return self._events[-limit:]

    def clear(self) -> None:
        self._events.clear()
