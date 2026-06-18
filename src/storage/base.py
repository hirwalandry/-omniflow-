from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..models.document import Document


@dataclass
class StorageResult:
    success: bool
    destination: str
    document_id: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class StorageHandler(ABC):
    def __init__(self, name: str, config: Optional[dict] = None):
        self.name = name
        self.config = config or {}

    @abstractmethod
    async def store(self, document: Document) -> StorageResult:
        ...

    @abstractmethod
    async def health_check(self) -> bool:
        ...


class BatchStorageEngine:
    def __init__(self, handlers: List[StorageHandler]):
        self.handlers = {h.name: h for h in handlers}

    async def store_all(self, document: Document) -> List[StorageResult]:
        results = []
        for name, handler in self.handlers.items():
            try:
                result = await handler.store(document)
                results.append(result)
            except Exception as e:
                results.append(
                    StorageResult(
                        success=False,
                        destination=name,
                        document_id=document.id,
                        error_message=str(e),
                    )
                )
        return results
