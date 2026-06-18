from __future__ import annotations
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class MetricType(Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMING = "timing"


class MetricSample(BaseModel):
    name: str
    metric_type: MetricType
    value: float
    tags: Dict[str, str] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    host: Optional[str] = None
    source: Optional[str] = None


class MetricBucket(BaseModel):
    name: str
    metric_type: MetricType
    count: int = 0
    sum: float = 0.0
    min: Optional[float] = None
    max: Optional[float] = None
    avg: Optional[float] = None
    p50: Optional[float] = None
    p95: Optional[float] = None
    p99: Optional[float] = None
    tags: Dict[str, str] = Field(default_factory=dict)
    window_start: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    window_end: Optional[datetime] = None


class PipelineRunMetrics(BaseModel):
    run_id: str = Field(default_factory=lambda: str(uuid4()))
    pipeline_name: str
    documents_processed: int = 0
    documents_succeeded: int = 0
    documents_failed: int = 0
    documents_rejected: int = 0
    total_duration_ms: Optional[float] = None
    stage_metrics: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    errors: List[Dict[str, Any]] = Field(default_factory=list)
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
