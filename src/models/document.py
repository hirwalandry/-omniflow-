from __future__ import annotations
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4, UUID

from pydantic import BaseModel, Field


class DocumentStatus(Enum):
    PENDING = "pending"
    INGESTED = "ingested"
    VALIDATED = "validated"
    TRANSFORMED = "transformed"
    ENRICHED = "enriched"
    ANALYZED = "analyzed"
    ROUTED = "routed"
    DELIVERED = "delivered"
    FAILED = "failed"
    REJECTED = "rejected"


class ProcessingStage(Enum):
    INGESTION = "ingestion"
    VALIDATION = "validation"
    TRANSFORMATION = "transformation"
    ENRICHMENT = "enrichment"
    ANALYSIS = "analysis"
    ROUTING = "routing"
    DELIVERY = "delivery"


class DocumentMetadata(BaseModel):
    source_name: str
    source_type: str
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    original_filename: Optional[str] = None
    content_type: Optional[str] = None
    content_length: Optional[int] = None
    checksum: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    custom: Dict[str, Any] = Field(default_factory=dict)


class ProcessingRecord(BaseModel):
    stage: ProcessingStage
    processor_name: str
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    duration_ms: Optional[float] = None
    status: str = "pending"
    error: Optional[str] = None
    output_schema_version: Optional[str] = None


class RoutingDirective(BaseModel):
    destination: str
    priority: int = 0
    ttl_seconds: Optional[int] = None
    transform_before_delivery: Optional[str] = None
    delivery_ack_required: bool = False


class Document(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    tenant_id: str
    pipeline_name: str
    status: DocumentStatus = DocumentStatus.PENDING
    metadata: DocumentMetadata
    payload: Optional[Dict[str, Any]] = None
    transformed_payload: Optional[Dict[str, Any]] = None
    enriched_payload: Optional[Dict[str, Any]] = None
    analysis_results: Optional[Dict[str, Any]] = None
    routing: Optional[RoutingDirective] = None
    processing_history: List[ProcessingRecord] = Field(default_factory=list)
    error_message: Optional[str] = None
    retry_count: int = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def add_processing_record(self, stage: ProcessingStage, processor_name: str) -> ProcessingRecord:
        record = ProcessingRecord(stage=stage, processor_name=processor_name)
        self.processing_history.append(record)
        return record

    def complete_processing_record(self, record: ProcessingRecord, status: str = "success", error: Optional[str] = None):
        record.completed_at = datetime.now(timezone.utc)
        record.duration_ms = (record.completed_at - record.started_at).total_seconds() * 1000
        record.status = status
        record.error = error
        self.updated_at = datetime.now(timezone.utc)

    def update_status(self, new_status: DocumentStatus):
        self.status = new_status
        self.updated_at = datetime.now(timezone.utc)
