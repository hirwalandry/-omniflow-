from __future__ import annotations
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from .base import StorageHandler, StorageResult
from ..models.document import Document, DocumentStatus, ProcessingStage
from ..config import settings


class DatabaseStorageHandler(StorageHandler):
    def __init__(self, name: str = "database", config: Optional[dict] = None):
        super().__init__(name, config)
        self._store: Dict[str, Dict[str, Any]] = {}
        self._documents: Dict[str, Document] = {}

    async def store(self, document: Document) -> StorageResult:
        record = {
            "id": document.id,
            "tenant_id": document.tenant_id,
            "pipeline_name": document.pipeline_name,
            "status": document.status.value,
            "source_name": document.metadata.source_name,
            "source_type": document.metadata.source_type,
            "original_filename": document.metadata.original_filename,
            "checksum": document.metadata.checksum,
            "tags": document.metadata.tags,
            "payload": document.transformed_payload or document.payload,
            "enriched_payload": document.enriched_payload,
            "analysis_results": document.analysis_results,
            "routing_destination": document.routing.destination if document.routing else None,
            "routing_priority": document.routing.priority if document.routing else 0,
            "error_message": document.error_message,
            "retry_count": document.retry_count,
            "created_at": document.created_at.isoformat(),
            "updated_at": document.updated_at.isoformat(),
            "stored_at": datetime.now(timezone.utc).isoformat(),
            "processing_history": [
                {
                    "stage": r.stage.value,
                    "processor": r.processor_name,
                    "status": r.status,
                    "duration_ms": r.duration_ms,
                    "error": r.error,
                    "started_at": r.started_at.isoformat(),
                    "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                }
                for r in document.processing_history
            ],
        }

        self._store[document.id] = record
        self._documents[document.id] = document

        return StorageResult(
            success=True,
            destination="in_memory_db",
            document_id=document.id,
            metadata={"record_count": len(self._store), "record_size_bytes": len(json.dumps(record))},
        )

    async def health_check(self) -> bool:
        return True

    async def get_document(self, doc_id: str) -> Optional[Document]:
        return self._documents.get(doc_id)

    async def get_record(self, doc_id: str) -> Optional[Dict[str, Any]]:
        return self._store.get(doc_id)

    async def query(self, status: Optional[DocumentStatus] = None, pipeline: Optional[str] = None, tenant: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        results = list(self._store.values())

        if status:
            results = [r for r in results if r["status"] == status.value]
        if pipeline:
            results = [r for r in results if r["pipeline_name"] == pipeline]
        if tenant:
            results = [r for r in results if r["tenant_id"] == tenant]

        return results[:limit]

    async def delete_document(self, doc_id: str) -> bool:
        self._store.pop(doc_id, None)
        self._documents.pop(doc_id, None)
        return True
