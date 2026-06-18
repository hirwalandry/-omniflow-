from __future__ import annotations
import json
import os
import tempfile

import pytest

from src.utils.pipeline_loader import load_pipeline_definition, load_all_pipeline_definitions
from src.utils.errors import ConfigurationError
from src.models.pipeline import PipelineStage


SAMPLE_PIPELINE = {
    "name": "test-pipeline",
    "version": "2.0.0",
    "description": "A test pipeline",
    "stages": [
        {
            "name": "validate",
            "stage": "validate",
            "processor_class": "SchemaValidator",
            "config": {"required_fields": ["name", "email"]},
        },
        {
            "name": "transform",
            "stage": "transform",
            "processor_class": "DataTransformer",
            "config": {"operations": [{"type": "coerce_type", "field": "age", "target_type": "int"}]},
            "depends_on": ["validate"],
        },
        {
            "name": "enrich",
            "stage": "enrich",
            "processor_class": "DataEnricher",
            "config": {"dedup_fields": ["email"]},
            "depends_on": ["transform"],
        },
        {
            "name": "route",
            "stage": "route",
            "processor_class": "DocumentRouter",
            "config": {"routes": [], "fallback_destination": "default"},
            "depends_on": ["enrich"],
        },
    ],
    "tags": ["test"],
}


class TestPipelineLoader:
    def test_load_pipeline(self, tmp_path):
        config_dir = os.path.join(tmp_path, "pipelines")
        os.makedirs(config_dir)
        path = os.path.join(config_dir, "test-pipeline.json")
        with open(path, "w") as f:
            json.dump(SAMPLE_PIPELINE, f)

        pipeline = load_pipeline_definition("test-pipeline", config_dir=config_dir)
        assert pipeline.name == "test-pipeline"
        assert pipeline.version == "2.0.0"
        assert len(pipeline.stages) == 4
        assert pipeline.stages[0].stage == PipelineStage.VALIDATE
        assert pipeline.stages[1].depends_on == ["validate"]

    def test_load_missing_pipeline(self):
        with pytest.raises(ConfigurationError, match="Pipeline definition not found"):
            load_pipeline_definition("nonexistent", config_dir="/tmp/nonexistent")

    def test_load_all_pipelines(self, tmp_path):
        config_dir = os.path.join(tmp_path, "pipelines")
        os.makedirs(config_dir)
        for name in ["pipe-a.json", "pipe-b.json"]:
            data = SAMPLE_PIPELINE.copy()
            data["name"] = name.replace(".json", "")
            with open(os.path.join(config_dir, name), "w") as f:
                json.dump(data, f)

        pipelines = load_all_pipeline_definitions(config_dir=config_dir)
        assert len(pipelines) == 2
        names = {p.name for p in pipelines}
        assert names == {"pipe-a", "pipe-b"}

    def test_dag_validation_cycle(self, tmp_path):
        bad_pipeline = {
            "name": "cyclic",
            "stages": [
                {"name": "a", "stage": "validate", "processor_class": "X", "depends_on": ["b"]},
                {"name": "b", "stage": "validate", "processor_class": "X", "depends_on": ["a"]},
            ],
        }
        config_dir = os.path.join(tmp_path, "pipelines")
        os.makedirs(config_dir)
        path = os.path.join(config_dir, "cyclic.json")
        with open(path, "w") as f:
            json.dump(bad_pipeline, f)

        with pytest.raises(ConfigurationError, match="circular"):
            load_pipeline_definition("cyclic", config_dir=config_dir)

    def test_invalid_stage_name(self, tmp_path):
        bad_pipeline = {
            "name": "bad",
            "stages": [
                {"name": "x", "stage": "nonexistent", "processor_class": "X"},
            ],
        }
        config_dir = os.path.join(tmp_path, "pipelines")
        os.makedirs(config_dir)
        path = os.path.join(config_dir, "bad.json")
        with open(path, "w") as f:
            json.dump(bad_pipeline, f)

        with pytest.raises(ConfigurationError, match="Unknown pipeline stage"):
            load_pipeline_definition("bad", config_dir=config_dir)

    def test_topological_sort(self):
        from src.models.pipeline import StageConfig, PipelineDefinition

        pipeline = PipelineDefinition(
            name="sorted",
            stages=[
                StageConfig(name="route", stage=PipelineStage.ROUTE, processor_class="R", depends_on=["enrich"]),
                StageConfig(name="validate", stage=PipelineStage.VALIDATE, processor_class="V"),
                StageConfig(name="enrich", stage=PipelineStage.ENRICH, processor_class="E", depends_on=["transform"]),
                StageConfig(name="transform", stage=PipelineStage.TRANSFORM, processor_class="T", depends_on=["validate"]),
            ],
        )
        sorted_stages = pipeline.topological_sort()
        sorted_names = [s.name for s in sorted_stages]
        validate_idx = sorted_names.index("validate")
        transform_idx = sorted_names.index("transform")
        enrich_idx = sorted_names.index("enrich")
        route_idx = sorted_names.index("route")
        assert validate_idx < transform_idx < enrich_idx < route_idx


class TestDAGValidation:
    def test_valid_dag(self):
        from src.models.pipeline import StageConfig, PipelineDefinition

        p = PipelineDefinition(
            name="valid",
            stages=[
                StageConfig(name="a", stage=PipelineStage.VALIDATE, processor_class="X"),
                StageConfig(name="b", stage=PipelineStage.TRANSFORM, processor_class="X", depends_on=["a"]),
                StageConfig(name="c", stage=PipelineStage.ENRICH, processor_class="X", depends_on=["b"]),
            ],
        )
        assert p.validate_dag() == []

    def test_self_dependency(self):
        from src.models.pipeline import StageConfig, PipelineDefinition

        p = PipelineDefinition(
            name="self-dep",
            stages=[
                StageConfig(name="a", stage=PipelineStage.VALIDATE, processor_class="X", depends_on=["a"]),
            ],
        )
        errors = p.validate_dag()
        assert any("depends on itself" in e for e in errors)

    def test_missing_dependency(self):
        from src.models.pipeline import StageConfig, PipelineDefinition

        p = PipelineDefinition(
            name="missing-dep",
            stages=[
                StageConfig(name="a", stage=PipelineStage.VALIDATE, processor_class="X", depends_on=["nonexistent"]),
            ],
        )
        errors = p.validate_dag()
        assert any("unknown" in e for e in errors)
