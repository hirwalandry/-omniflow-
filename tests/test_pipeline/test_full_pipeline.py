from __future__ import annotations
import os
import json
import tempfile
from typing import Any, Dict
from uuid import uuid4

import pytest

from src.models.document import Document, DocumentMetadata, DocumentStatus, ProcessingStage
from src.models.pipeline import StageConfig, PipelineStage
from src.ingestion.file_source import FileSourceHandler
from src.processing.validator import SchemaValidator
from src.processing.transformer import DataTransformer
from src.processing.enricher import DataEnricher
from src.processing.router import DocumentRouter
from src.processing.base import PipelineExecutor
from src.storage.console import ConsoleStorageHandler
from src.storage.file_storage import FileStorageHandler
from src.storage.database import DatabaseStorageHandler
from src.monitoring.metrics_collector import MetricsCollector
from src.monitoring.audit import AuditLogger


@pytest.fixture
def sample_payload() -> Dict[str, Any]:
    return {
        "name": "John Doe",
        "email": "john@example.com",
        "age": "30",
        "score": "95.5",
        "department": "engineering",
        "active": "true",
        "address": {
            "street": "123 Main St",
            "city": "Portland",
            "zip": "97201",
        },
        "skills": ["python", "rust", "docker"],
    }


@pytest.fixture
def input_file(tmp_path, sample_payload) -> str:
    filepath = os.path.join(tmp_path, "test_doc.json")
    with open(filepath, "w") as f:
        json.dump(sample_payload, f)
    return filepath


@pytest.fixture
def document(sample_payload) -> Document:
    return Document(
        tenant_id="test-tenant",
        pipeline_name="test-pipeline",
        metadata=DocumentMetadata(
            source_name="test",
            source_type="test",
        ),
        payload=sample_payload,
    )


class TestFullPipeline:
    async def test_file_ingestion(self, input_file):
        handler = FileSourceHandler("test-source", watch_dir=os.path.dirname(input_file))
        ids = await handler.discover()
        assert len(ids) == 1
        assert ids[0] == input_file

        doc = await handler.fetch(ids[0])
        assert doc.tenant_id == "default"
        assert doc.pipeline_name == "default-pipeline"
        assert doc.status == DocumentStatus.PENDING
        assert doc.metadata.source_type == "file"
        assert doc.metadata.checksum is not None
        assert doc.payload["name"] == "John Doe"

    async def test_validation_pass(self, document):
        config = StageConfig(
            name="validator",
            stage=PipelineStage.VALIDATE,
            config={
                "required_fields": ["name", "email", "age"],
                "field_constraints": {
                    "email": {"type": "str"},
                    "age": {"type": "str"},
                },
            },
        )
        validator = SchemaValidator(config)
        result = await validator.process(document)
        assert result.status == DocumentStatus.PENDING

    async def test_validation_fail(self, document):
        config = StageConfig(
            name="validator",
            stage=PipelineStage.VALIDATE,
            config={"required_fields": ["name", "nonexistent_field"]},
        )
        validator = SchemaValidator(config)
        with pytest.raises(Exception, match="Schema validation failed"):
            await validator.process(document)

    async def test_transformation(self, document):
        config = StageConfig(
            name="transformer",
            stage=PipelineStage.TRANSFORM,
            config={
                "operations": [
                    {"type": "rename_field", "from": "name", "to": "full_name"},
                    {"type": "coerce_type", "field": "age", "target_type": "int"},
                    {"type": "coerce_type", "field": "score", "target_type": "float"},
                    {"type": "coerce_type", "field": "active", "target_type": "bool"},
                    {"type": "default_value", "field": "missing_field", "default": "fallback"},
                ]
            },
        )
        transformer = DataTransformer(config)
        result = await transformer.process(document)
        assert "full_name" in result.transformed_payload
        assert "name" not in result.transformed_payload
        assert isinstance(result.transformed_payload["age"], int)
        assert isinstance(result.transformed_payload["score"], float)
        assert isinstance(result.transformed_payload["active"], bool)
        assert result.transformed_payload["missing_field"] == "fallback"

    async def test_enrichment_dedup_and_entities(self, document):
        document.transformed_payload = document.payload
        config = StageConfig(
            name="enricher",
            stage=PipelineStage.ENRICH,
            config={
                "dedup_fields": ["email"],
                "extract_entities": True,
                "entity_types": ["email", "phone"],
                "tag_sources": ["department"],
            },
        )
        enricher = DataEnricher(config)
        result = await enricher.process(document)

        assert result.enriched_payload is not None
        assert "_dedup" in result.enriched_payload
        assert result.enriched_payload["_dedup"]["email"]["duplicate"] is False
        assert "_entities" in result.enriched_payload
        assert "email" in result.enriched_payload["_entities"]
        assert "engineering" in result.metadata.tags

    async def test_enrichment_duplicate_detection(self, document):
        document.transformed_payload = {"email": "dup@example.com"}
        config = StageConfig(
            name="enricher",
            stage=PipelineStage.ENRICH,
            config={"dedup_fields": ["email"]},
        )
        enricher = DataEnricher(config)

        await enricher.process(document)
        doc2 = Document(
            tenant_id="test",
            pipeline_name="test",
            metadata=DocumentMetadata(source_name="test", source_type="test"),
            payload={"email": "dup@example.com"},
            transformed_payload={"email": "dup@example.com"},
        )
        result2 = await enricher.process(doc2)
        assert result2.enriched_payload["_dedup"]["email"]["duplicate"] is True
        assert result2.enriched_payload["_dedup"]["is_duplicate"] is True

    async def test_routing_tenant_based(self, document):
        document.transformed_payload = document.payload
        config = StageConfig(
            name="router",
            stage=PipelineStage.ROUTE,
            config={
                "routes": [
                    {
                        "type": "tenant_based",
                        "condition": {"value": "test-tenant", "operator": "equals"},
                        "destination": "tenant-storage",
                    }
                ],
                "fallback_destination": "default",
            },
        )
        router = DocumentRouter(config)
        result = await router.process(document)
        assert result.routing is not None
        assert result.routing.destination == "tenant-storage"

    async def test_routing_fallback(self, document):
        document.tenant_id = "unknown-tenant"
        document.transformed_payload = document.payload
        config = StageConfig(
            name="router",
            stage=PipelineStage.ROUTE,
            config={
                "routes": [
                    {
                        "type": "tenant_based",
                        "condition": {"value": "other-tenant", "operator": "equals"},
                        "destination": "other-storage",
                    }
                ],
                "fallback_destination": "default-queue",
            },
        )
        router = DocumentRouter(config)
        result = await router.process(document)
        assert result.routing.destination == "default-queue"

    async def test_full_pipeline_execution(self, document):
        stage_configs = {
            "validator": StageConfig(
                name="validator",
                stage=PipelineStage.VALIDATE,
                config={"required_fields": ["name", "email"]},
            ),
            "transformer": StageConfig(
                name="transformer",
                stage=PipelineStage.TRANSFORM,
                config={
                    "operations": [
                        {"type": "coerce_type", "field": "age", "target_type": "int"},
                        {"type": "coerce_type", "field": "score", "target_type": "float"},
                    ]
                },
            ),
            "enricher": StageConfig(
                name="enricher",
                stage=PipelineStage.ENRICH,
                config={
                    "dedup_fields": ["email"],
                    "tag_sources": ["department"],
                },
            ),
            "router": StageConfig(
                name="router",
                stage=PipelineStage.ROUTE,
                config={
                    "routes": [
                        {
                            "type": "tenant_based",
                            "condition": {"value": "test-tenant", "operator": "equals"},
                            "destination": "final-storage",
                        }
                    ],
                    "fallback_destination": "default",
                },
            ),
        }

        executor = PipelineExecutor(
            processors={
                "validator": SchemaValidator(stage_configs["validator"]),
                "transformer": DataTransformer(stage_configs["transformer"]),
                "enricher": DataEnricher(stage_configs["enricher"]),
                "router": DocumentRouter(stage_configs["router"]),
            }
        )

        result = await executor.execute(document, ["validator", "transformer", "enricher", "router"])

        assert result.status == DocumentStatus.ROUTED
        assert result.transformed_payload is not None
        assert isinstance(result.transformed_payload["age"], int)
        assert result.enriched_payload is not None
        assert "_dedup" in result.enriched_payload
        assert result.routing is not None
        assert result.routing.destination == "final-storage"
        assert len(result.processing_history) == 4
        for record in result.processing_history:
            assert record.status == "success"

    async def test_full_pipeline_with_storage(self, document, tmp_path):
        stage_configs = {
            "validator": StageConfig(
                name="validator",
                stage=PipelineStage.VALIDATE,
                config={"required_fields": ["name", "email"]},
            ),
            "transformer": StageConfig(
                name="transformer",
                stage=PipelineStage.TRANSFORM,
                config={"operations": [{"type": "coerce_type", "field": "age", "target_type": "int"}]},
            ),
            "router": StageConfig(
                name="router",
                stage=PipelineStage.ROUTE,
                config={
                    "routes": [
                        {
                            "type": "tenant_based",
                            "condition": {"value": "test-tenant", "operator": "equals"},
                            "destination": "final-storage",
                        }
                    ],
                    "fallback_destination": "default",
                },
            ),
        }

        executor = PipelineExecutor(
            processors={
                "validator": SchemaValidator(stage_configs["validator"]),
                "transformer": DataTransformer(stage_configs["transformer"]),
                "router": DocumentRouter(stage_configs["router"]),
            }
        )

        output_dir = os.path.join(tmp_path, "output")

        result = await executor.execute(document, ["validator", "transformer", "router"])
        assert result.status == DocumentStatus.ROUTED

        console_handler = ConsoleStorageHandler("console")
        console_result = await console_handler.store(result)
        assert console_result.success is True

        file_handler = FileStorageHandler("file", output_dir=output_dir)
        file_result = await file_handler.store(result)
        assert file_result.success is True
        assert os.path.exists(file_result.metadata["path"])

        db_handler = DatabaseStorageHandler("db")
        db_result = await db_handler.store(result)
        assert db_result.success is True
        retrieved = await db_handler.get_document(result.id)
        assert retrieved is not None
        assert retrieved.id == result.id


class TestMetricsCollector:
    def test_increment(self):
        m = MetricsCollector()
        m.increment("docs_processed")
        m.increment("docs_processed")
        assert m.get_count("docs_processed") == 2

    def test_gauge(self):
        m = MetricsCollector()
        m.gauge("queue_depth", 42)
        assert m.get_gauge("queue_depth") == 42

    def test_timing(self):
        m = MetricsCollector()
        m.timing("process_ms", 100)
        m.timing("process_ms", 200)
        stats = m.get_timing_stats("process_ms")
        assert stats["count"] == 2
        assert stats["avg"] == 150

    def test_timing_context(self):
        m = MetricsCollector()
        import time as t
        with m.timing_context("op_ms"):
            pass
        stats = m.get_timing_stats("op_ms")
        assert stats["count"] == 1
        assert stats["min"] < 100

    def test_reset(self):
        m = MetricsCollector()
        m.increment("x")
        m.reset()
        assert m.get_count("x") == 0

    def test_export(self):
        m = MetricsCollector()
        m.increment("a", labels={"env": "test"})
        exported = m.export_json()
        assert "a" in exported
        assert "counter" in exported


class TestAuditLogger:
    def test_log_and_retrieve(self, tmp_path):
        log_path = os.path.join(tmp_path, "audit.log")
        audit = AuditLogger(log_path)
        audit.log_ingestion("doc-1", "file-source", True)
        audit.log_processing("doc-1", "validation", "schema-validator", "success", 10.5)
        audit.log_storage("doc-1", "console", True)
        audit.log_error("doc-1", "transformation", "field missing")

        events = audit.get_recent_events(10)
        assert len(events) == 4
        assert events[0]["event_type"] == "ingestion"
        assert events[1]["event_type"] == "processing"
        assert events[3]["event_type"] == "error"

        assert os.path.exists(log_path)
        with open(log_path) as f:
            lines = f.readlines()
        assert len(lines) == 4

    def test_clear(self):
        audit = AuditLogger()
        audit.log_ingestion("doc-1", "src", True)
        assert len(audit.get_recent_events()) == 1
        audit.clear()
        assert len(audit.get_recent_events()) == 0


class TestDatabaseStorage:
    async def test_crud(self):
        db = DatabaseStorageHandler()
        doc = Document(
            tenant_id="t1",
            pipeline_name="p1",
            metadata=DocumentMetadata(source_name="s1", source_type="csv"),
            payload={"key": "value"},
        )

        result = await db.store(doc)
        assert result.success is True

        retrieved = await db.get_document(doc.id)
        assert retrieved is not None
        assert retrieved.id == doc.id

        record = await db.get_record(doc.id)
        assert record is not None
        assert record["payload"]["key"] == "value"

        await db.delete_document(doc.id)
        assert await db.get_document(doc.id) is None

    async def test_query(self):
        db = DatabaseStorageHandler()
        for i in range(5):
            doc = Document(
                tenant_id="t1",
                pipeline_name="p1",
                metadata=DocumentMetadata(source_name="s1", source_type="csv"),
                payload={"idx": i},
            )
            await db.store(doc)

        results = await db.query(limit=3)
        assert len(results) == 3

        second_batch = await db.query(limit=10)
        assert len(second_batch) == 5


class TestFileStorage:
    async def test_store_and_health(self, tmp_path):
        output = os.path.join(tmp_path, "out")
        fs = FileStorageHandler("file", output_dir=output, config={"create_sidecar": True})
        doc = Document(
            tenant_id="t1",
            pipeline_name="p1",
            metadata=DocumentMetadata(source_name="s1", source_type="csv"),
            payload={"msg": "hello"},
        )

        result = await fs.store(doc)
        assert result.success is True
        assert os.path.exists(result.metadata["path"])

        meta_path = os.path.join(output, f"{doc.id}.meta.json")
        assert os.path.exists(meta_path)

        assert await fs.health_check() is True


class TestConsoleStorage:
    async def test_store(self, capsys):
        cs = ConsoleStorageHandler()
        doc = Document(
            tenant_id="t1",
            pipeline_name="p1",
            metadata=DocumentMetadata(source_name="s1", source_type="test"),
            payload={"key": "value"},
        )
        result = await cs.store(doc)
        assert result.success is True
        captured = capsys.readouterr()
        assert doc.id in captured.out
