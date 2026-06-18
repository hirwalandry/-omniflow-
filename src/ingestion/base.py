from __future__ import annotations
from abc import ABC, abstractmethod
from typing import AsyncIterator, List, Optional

from ..models.document import Document, ProcessingStage, DocumentStatus


class SourceHandler(ABC):
    def __init__(self, source_name: str, config: Optional[dict] = None):
        self.source_name = source_name
        self.config = config or {}

    @abstractmethod
    async def discover(self) -> List[str]:
        ...

    @abstractmethod
    async def fetch(self, source_id: str) -> Document:
        ...

    @abstractmethod
    async def stream(self) -> AsyncIterator[Document]:
        ...

    @abstractmethod
    async def acknowledge(self, source_id: str, success: bool) -> bool:
        ...

    @abstractmethod
    async def health_check(self) -> bool:
        ...


class BatchIngestionEngine:
    def __init__(self, handlers: List[SourceHandler]):
        self.handlers = {h.source_name: h for h in handlers}

    async def ingest_all(self) -> List[Document]:
        documents = []
        for name, handler in self.handlers.items():
            try:
                source_ids = await handler.discover()
                for sid in source_ids:
                    try:
                        doc = await handler.fetch(sid)
                        doc.update_status(DocumentStatus.INGESTED)
                        doc.add_processing_record(stage=ProcessingStage.INGESTION, processor_name=handler.source_name)
                        documents.append(doc)
                        await handler.acknowledge(sid, True)
                    except Exception as e:
                        await handler.acknowledge(sid, False)
                        raise e
            except Exception as e:
                print(f"[Ingestion] Handler {name} failed: {e}")
        return documents

    async def stream_all(self):
        for name, handler in self.handlers.items():
            async for doc in handler.stream():
                yield doc
