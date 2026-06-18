from __future__ import annotations
from abc import ABC, abstractmethod
from typing import AsyncIterator, Dict, List, Optional

from ..models.document import Document, DocumentStatus, ProcessingStage
from ..models.pipeline import StageConfig
from ..monitoring.metrics_collector import MetricsCollector


class Processor(ABC):
    def __init__(self, stage_config: StageConfig, metrics: Optional[MetricsCollector] = None):
        self.config = stage_config
        self.metrics = metrics or MetricsCollector()

    @abstractmethod
    async def process(self, document: Document) -> Document:
        ...

    @abstractmethod
    async def validate(self, document: Document) -> bool:
        ...

    @property
    @abstractmethod
    def stage(self) -> ProcessingStage:
        ...


class PipelineExecutor:
    STATUS_MAP = {
        ProcessingStage.VALIDATION: DocumentStatus.VALIDATED,
        ProcessingStage.TRANSFORMATION: DocumentStatus.TRANSFORMED,
        ProcessingStage.ENRICHMENT: DocumentStatus.ENRICHED,
        ProcessingStage.ANALYSIS: DocumentStatus.ANALYZED,
        ProcessingStage.ROUTING: DocumentStatus.ROUTED,
        ProcessingStage.DELIVERY: DocumentStatus.DELIVERED,
    }

    def __init__(self, processors: Dict[str, Processor]):
        self.processors = processors

    async def execute(self, document: Document, stage_names: List[str]) -> Document:
        for stage_name in stage_names:
            processor = self.processors.get(stage_name)
            if not processor:
                raise ValueError(f"Unknown processor stage: {stage_name}")

            if not await processor.validate(document):
                document.update_status(DocumentStatus.REJECTED)
                document.error_message = f"Validation failed at stage '{stage_name}'"
                break

            record = document.add_processing_record(stage=processor.stage, processor_name=stage_name)

            try:
                document = await processor.process(document)
                document.complete_processing_record(record, status="success")
                new_status = self.STATUS_MAP.get(processor.stage, DocumentStatus.DELIVERED)
                document.update_status(new_status)
            except Exception as e:
                document.complete_processing_record(record, status="failed", error=str(e))
                document.update_status(DocumentStatus.FAILED)
                document.error_message = str(e)
                break

        return document

    async def execute_stream(self, documents: AsyncIterator[Document], stage_names: List[str]) -> AsyncIterator[Document]:
        async for document in documents:
            yield await self.execute(document, stage_names)
