from __future__ import annotations
import os
import json
from datetime import datetime, timezone
from typing import Any, Optional

from .base import StorageHandler, StorageResult
from ..models.document import Document
from ..utils.serialization import SerializationFormat, Serializer
from ..config import settings


class FileStorageHandler(StorageHandler):
    def __init__(self, name: str = "file", output_dir: Optional[str] = None, config: Optional[dict] = None):
        super().__init__(name, config)
        self.output_dir = output_dir or settings.data_output_dir
        self._serializer = Serializer()
        self.format = SerializationFormat(self.config.get("format", "json")) if config else SerializationFormat.JSON
        self.create_sidecar = config.get("create_sidecar", True) if config else True

    async def store(self, document: Document) -> StorageResult:
        os.makedirs(self.output_dir, exist_ok=True)

        filename = f"{document.id}.{self.format.value}"
        filepath = os.path.join(self.output_dir, filename)

        payload = document.transformed_payload or document.payload
        data = self._serializer.serialize(payload, self.format)

        with open(filepath, "wb") as f:
            f.write(data)

        if self.create_sidecar:
            meta = {
                "id": document.id,
                "tenant_id": document.tenant_id,
                "pipeline": document.pipeline_name,
                "status": document.status.value,
                "source": document.metadata.source_name,
                "checksum": document.metadata.checksum,
                "tags": document.metadata.tags,
                "routing": document.routing.model_dump() if document.routing else None,
                "processing": [
                    {
                        "stage": r.stage.value,
                        "processor": r.processor_name,
                        "status": r.status,
                        "duration_ms": r.duration_ms,
                    }
                    for r in document.processing_history
                ],
                "stored_at": datetime.now(timezone.utc).isoformat(),
            }
            meta_path = os.path.join(self.output_dir, f"{document.id}.meta.json")
            with open(meta_path, "w") as f:
                json.dump(meta, f, indent=2)

        return StorageResult(
            success=True,
            destination=filepath,
            document_id=document.id,
            metadata={"path": filepath, "format": self.format.value, "size_bytes": len(data)},
        )

    async def health_check(self) -> bool:
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            return True
        except OSError:
            return False
