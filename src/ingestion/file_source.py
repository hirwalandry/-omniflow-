from __future__ import annotations
import os
import hashlib
from typing import AsyncIterator, List, Optional

from .base import SourceHandler
from ..models.document import Document, DocumentMetadata
from ..utils.serialization import SerializationFormat, Serializer
from ..config import settings


class FileSourceHandler(SourceHandler):
    def __init__(self, source_name: str, watch_dir: Optional[str] = None, config: Optional[dict] = None):
        super().__init__(source_name, config)
        self.watch_dir = watch_dir or settings.data_input_dir
        self._serializer = Serializer()
        self._processed_files: set = set()

    async def discover(self) -> List[str]:
        if not os.path.isdir(self.watch_dir):
            return []
        files = []
        for entry in os.scandir(self.watch_dir):
            if entry.is_file() and entry.name not in self._processed_files:
                if entry.name.endswith((".json", ".yaml", ".yml", ".csv")):
                    files.append(entry.path)
        return files

    async def fetch(self, source_id: str) -> Document:
        filepath = source_id
        filename = os.path.basename(filepath)
        ext = os.path.splitext(filepath)[1].lower()
        fmt_map = {".json": SerializationFormat.JSON, ".yaml": SerializationFormat.YAML, ".yml": SerializationFormat.YAML, ".csv": SerializationFormat.CSV}
        fmt = fmt_map.get(ext, SerializationFormat.JSON)

        with open(filepath, "rb") as f:
            raw = f.read()

        checksum = hashlib.sha256(raw).hexdigest()
        payload = self._serializer.deserialize(raw, fmt)

        metadata = DocumentMetadata(
            source_name=self.source_name,
            source_type="file",
            original_filename=filename,
            content_type=f"application/{ext.lstrip('.')}",
            content_length=len(raw),
            checksum=checksum,
            tags=["file-ingest", ext.lstrip(".")],
        )

        tenant = self.config.get("tenant_id", "default")
        pipeline = self.config.get("pipeline", "default-pipeline")

        return Document(
            tenant_id=tenant,
            pipeline_name=pipeline,
            metadata=metadata,
            payload=payload if isinstance(payload, dict) else {"data": payload},
        )

    async def stream(self) -> AsyncIterator[Document]:
        files = await self.discover()
        for fpath in files:
            doc = await self.fetch(fpath)
            self._processed_files.add(os.path.basename(fpath))
            yield doc

    async def acknowledge(self, source_id: str, success: bool) -> bool:
        if success:
            self._processed_files.add(os.path.basename(source_id))
        return True

    async def health_check(self) -> bool:
        return os.path.isdir(self.watch_dir)
