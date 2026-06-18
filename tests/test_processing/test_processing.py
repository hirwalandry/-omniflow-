from __future__ import annotations
import pytest

from src.processing.validator import SchemaValidator
from src.processing.transformer import DataTransformer
from src.processing.enricher import DataEnricher
from src.processing.router import DocumentRouter
from src.processing.base import PipelineExecutor
from src.models.document import Document, DocumentMetadata, DocumentStatus, ProcessingStage
from src.models.pipeline import StageConfig, PipelineStage


@pytest.fixture
def doc() -> Document:
    return Document(
        tenant_id="tenant-1",
        pipeline_name="test",
        metadata=DocumentMetadata(source_name="src", source_type="csv"),
        payload={
            "name": "Alice",
            "email": "alice@example.com",
            "age": "30",
            "score": "85.5",
            "department": "engineering",
        },
    )


class TestSchemaValidator:
    @pytest.fixture
    def validator(self):
        return SchemaValidator(StageConfig(
            name="validate",
            stage=PipelineStage.VALIDATE,
            config={
                "required_fields": ["name", "email"],
                "field_constraints": {
                    "name": {"type": "str", "min_length": 2},
                    "age": {"type": "str"},
                },
            },
        ))

    async def test_validate_pass(self, validator, doc):
        assert await validator.validate(doc) is True

    async def test_validate_fail_no_payload(self, validator):
        empty = Document(
            tenant_id="t", pipeline_name="p",
            metadata=DocumentMetadata(source_name="s", source_type="t"),
            payload=None,
        )
        assert await validator.validate(empty) is False

    async def test_process_pass(self, validator, doc):
        result = await validator.process(doc)
        assert result is doc

    async def test_process_missing_required(self, doc):
        v = SchemaValidator(StageConfig(
            name="v", stage=PipelineStage.VALIDATE,
            config={"required_fields": ["nonexistent"]},
        ))
        with pytest.raises(Exception, match="Schema validation failed"):
            await v.process(doc)

    async def test_field_constraints_str_length(self, doc):
        v = SchemaValidator(StageConfig(
            name="v", stage=PipelineStage.VALIDATE,
            config={"field_constraints": {"name": {"min_length": 10}}},
        ))
        with pytest.raises(Exception, match="below minimum length"):
            await v.process(doc)

    async def test_field_constraints_pattern(self, doc):
        v = SchemaValidator(StageConfig(
            name="v", stage=PipelineStage.VALIDATE,
            config={"field_constraints": {"name": {"pattern": r"^\d+$"}}},
        ))
        with pytest.raises(Exception, match="does not match pattern"):
            await v.process(doc)


class TestDataTransformer:
    @pytest.fixture
    def transformer(self):
        return DataTransformer(StageConfig(
            name="transform",
            stage=PipelineStage.TRANSFORM,
            config={
                "operations": [
                    {"type": "coerce_type", "field": "age", "target_type": "int"},
                    {"type": "coerce_type", "field": "score", "target_type": "float"},
                    {"type": "rename_field", "from": "name", "to": "full_name"},
                    {"type": "default_value", "field": "missing", "default": "N/A"},
                ]
            },
        ))

    async def test_transform_operations(self, transformer, doc):
        result = await transformer.process(doc)
        tp = result.transformed_payload
        assert isinstance(tp["age"], int)
        assert isinstance(tp["score"], float)
        assert "full_name" in tp
        assert "name" not in tp
        assert tp["missing"] == "N/A"

    async def test_validate(self, transformer, doc):
        assert await transformer.validate(doc) is True

    async def test_validate_no_payload(self, transformer):
        empty = Document(
            tenant_id="t", pipeline_name="p",
            metadata=DocumentMetadata(source_name="s", source_type="t"),
            payload=None,
        )
        assert await transformer.validate(empty) is False

    async def test_compute_field(self, doc):
        t = DataTransformer(StageConfig(
            name="t", stage=PipelineStage.TRANSFORM,
            config={"operations": [{"type": "compute_field", "target_field": "name_length", "expression": "len(name)"}]},
        ))
        result = await t.process(doc)
        assert result.transformed_payload["name_length"] == 5

    async def test_remove_field(self, doc):
        t = DataTransformer(StageConfig(
            name="t", stage=PipelineStage.TRANSFORM,
            config={"operations": [{"type": "remove_field", "field": "age"}]},
        ))
        result = await t.process(doc)
        assert "age" not in result.transformed_payload

    async def test_flatten(self, doc):
        doc.payload["nested"] = {"x": {"y": 1}, "z": [{"a": 2}]}
        t = DataTransformer(StageConfig(
            name="t", stage=PipelineStage.TRANSFORM,
            config={"operations": [{"type": "flatten", "max_depth": 2}]},
        ))
        result = await t.process(doc)
        tp = result.transformed_payload
        assert "nested.x.y" in tp
        assert tp["nested.x.y"] == 1

    async def test_map_values(self, doc):
        t = DataTransformer(StageConfig(
            name="t", stage=PipelineStage.TRANSFORM,
            config={"operations": [{"type": "map_values", "field": "department", "mapping": {"engineering": "eng"}}]},
        ))
        result = await t.process(doc)
        assert result.transformed_payload["department"] == "eng"


class TestDataEnricher:
    async def test_dedup_no_duplicate(self, doc):
        doc.transformed_payload = doc.payload
        e = DataEnricher(StageConfig(
            name="enrich", stage=PipelineStage.ENRICH,
            config={"dedup_fields": ["email"]},
        ))
        result = await e.process(doc)
        assert result.enriched_payload["_dedup"]["email"]["duplicate"] is False
        assert result.enriched_payload["_dedup"]["is_duplicate"] is False

    async def test_dedup_detects_duplicate(self, doc):
        doc.transformed_payload = doc.payload
        e = DataEnricher(StageConfig(
            name="enrich", stage=PipelineStage.ENRICH,
            config={"dedup_fields": ["email"]},
        ))
        await e.process(doc)

        doc2 = Document(
            tenant_id="t", pipeline_name="p",
            metadata=DocumentMetadata(source_name="s", source_type="t"),
            payload={"email": "alice@example.com"},
            transformed_payload={"email": "alice@example.com"},
        )
        result2 = await e.process(doc2)
        assert result2.enriched_payload["_dedup"]["email"]["duplicate"] is True
        assert result2.enriched_payload["_dedup"]["is_duplicate"] is True

    async def test_entity_extraction(self, doc):
        doc.transformed_payload = doc.payload
        e = DataEnricher(StageConfig(
            name="enrich", stage=PipelineStage.ENRICH,
            config={"dedup_fields": ["email"]},
        ))
        result = await e.process(doc)
        assert "email" in result.enriched_payload["_entities"]
        assert "alice@example.com" in result.enriched_payload["_entities"]["email"]

    async def test_tag_source_fields(self, doc):
        doc.transformed_payload = doc.payload
        e = DataEnricher(StageConfig(
            name="enrich", stage=PipelineStage.ENRICH,
            config={"tag_sources": ["department"]},
        ))
        result = await e.process(doc)
        assert "engineering" in result.metadata.tags

    async def test_validate(self, doc):
        e = DataEnricher(StageConfig(name="e", stage=PipelineStage.ENRICH, config={}))
        assert await e.validate(doc) is True

    async def test_validate_no_payload(self):
        e = DataEnricher(StageConfig(name="e", stage=PipelineStage.ENRICH, config={}))
        empty = Document(
            tenant_id="t", pipeline_name="p",
            metadata=DocumentMetadata(source_name="s", source_type="t"),
            payload=None,
        )
        assert await e.validate(empty) is False


class TestDocumentRouter:
    async def test_tenant_based_routing(self, doc):
        doc.transformed_payload = doc.payload
        r = DocumentRouter(StageConfig(
            name="route", stage=PipelineStage.ROUTE,
            config={
                "routes": [{"type": "tenant_based", "condition": {"value": "tenant-1", "operator": "equals"}, "destination": "tenant-1-store"}],
                "fallback_destination": "default",
            },
        ))
        result = await r.process(doc)
        assert result.routing.destination == "tenant-1-store"

    async def test_fallback_routing(self, doc):
        doc.tenant_id = "unknown"
        doc.transformed_payload = doc.payload
        r = DocumentRouter(StageConfig(
            name="route", stage=PipelineStage.ROUTE,
            config={
                "routes": [{"type": "tenant_based", "condition": {"value": "tenant-1", "operator": "equals"}, "destination": "t1"}],
                "fallback_destination": "fallback-queue",
            },
        ))
        result = await r.process(doc)
        assert result.routing.destination == "fallback-queue"

    async def test_payload_based_routing(self, doc):
        doc.transformed_payload = doc.payload
        r = DocumentRouter(StageConfig(
            name="route", stage=PipelineStage.ROUTE,
            config={
                "routes": [{"type": "payload_based", "condition": {"field": "department", "value": "engineering", "operator": "equals"}, "destination": "eng-bucket"}],
                "fallback_destination": "default",
            },
        ))
        result = await r.process(doc)
        assert result.routing.destination == "eng-bucket"

    async def test_pattern_based_routing(self, doc):
        doc.transformed_payload = doc.payload
        r = DocumentRouter(StageConfig(
            name="route", stage=PipelineStage.ROUTE,
            config={
                "routes": [{"type": "pattern_based", "condition": {"field": "email", "pattern": r"alice@"}, "destination": "alice-inbox"}],
                "fallback_destination": "default",
            },
        ))
        result = await r.process(doc)
        assert result.routing.destination == "alice-inbox"

    async def test_tag_based_routing(self, doc):
        doc.metadata.tags = ["urgent", "finance"]
        doc.transformed_payload = doc.payload
        r = DocumentRouter(StageConfig(
            name="route", stage=PipelineStage.ROUTE,
            config={
                "routes": [{"type": "tag_based", "condition": {"tags": ["urgent"], "match_all": True}, "destination": "priority-queue"}],
                "fallback_destination": "default",
            },
        ))
        result = await r.process(doc)
        assert result.routing.destination == "priority-queue"

    async def test_validate_wrong_status(self, doc):
        r = DocumentRouter(StageConfig(name="route", stage=PipelineStage.ROUTE, config={}))
        assert await r.validate(doc) is False


class TestPipelineExecutor:
    async def test_full_execution(self, doc):
        executor = PipelineExecutor({
            "validate": SchemaValidator(StageConfig(
                name="v", stage=PipelineStage.VALIDATE,
                config={"required_fields": ["name", "email"]},
            )),
            "transform": DataTransformer(StageConfig(
                name="t", stage=PipelineStage.TRANSFORM,
                config={"operations": [{"type": "coerce_type", "field": "age", "target_type": "int"}]},
            )),
        })

        result = await executor.execute(doc, ["validate", "transform"])
        assert result.status == DocumentStatus.TRANSFORMED
        assert isinstance(result.transformed_payload["age"], int)

    async def test_validation_rejection(self, doc):
        executor = PipelineExecutor({
            "validate": SchemaValidator(StageConfig(
                name="v", stage=PipelineStage.VALIDATE,
                config={"required_fields": ["missing_field"]},
            )),
        })
        result = await executor.execute(doc, ["validate"])
        assert result.status == DocumentStatus.FAILED

    async def test_unknown_stage(self, doc):
        executor = PipelineExecutor({})
        with pytest.raises(ValueError, match="Unknown processor stage"):
            await executor.execute(doc, ["nonexistent"])

    async def test_execute_stream(self, doc):
        executor = PipelineExecutor({
            "validate": SchemaValidator(StageConfig(
                name="v", stage=PipelineStage.VALIDATE,
                config={"required_fields": ["name"]},
            )),
        })

        async def doc_stream():
            yield doc
            yield Document(
                tenant_id="t", pipeline_name="p",
                metadata=DocumentMetadata(source_name="s", source_type="t"),
                payload={"name": "Bob"},
            )

        results = []
        async for result in executor.execute_stream(doc_stream(), ["validate"]):
            results.append(result)

        assert len(results) == 2
        assert results[0].status == DocumentStatus.VALIDATED
        assert results[1].status == DocumentStatus.VALIDATED
