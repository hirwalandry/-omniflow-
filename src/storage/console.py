from __future__ import annotations
import json
from datetime import datetime, timezone
from typing import Any, Optional

from .base import StorageHandler, StorageResult
from ..models.document import Document


class ConsoleStorageHandler(StorageHandler):
    def __init__(self, name: str = "console", config: Optional[dict] = None):
        super().__init__(name, config)
        self.pretty_print = config.get("pretty_print", True) if config else True

    async def store(self, document: Document) -> StorageResult:
        output = {
            "id": document.id,
            "tenant_id": document.tenant_id,
            "pipeline": document.pipeline_name,
            "status": document.status.value,
            "metadata": document.metadata.model_dump(),
            "payload": document.transformed_payload or document.payload,
            "enriched": document.enriched_payload,
            "routing": document.routing.model_dump() if document.routing else None,
            "processing_history": [
                {
                    "stage": r.stage.value,
                    "processor": r.processor_name,
                    "status": r.status,
                    "duration_ms": r.duration_ms,
                }
                for r in document.processing_history
            ],
            "error": document.error_message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if self.pretty_print:
            print(f"\n{'='*60}")
            print(f"STORED: {document.id}")
            print(f"{'='*60}")
            print(json.dumps(output, indent=2, default=str))
            print(f"{'='*60}\n")
        else:
            print(json.dumps(output, default=str))

        return StorageResult(
            success=True,
            destination="console",
            document_id=document.id,
        )

    async def health_check(self) -> bool:
        return True
