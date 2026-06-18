from __future__ import annotations
import json
import os
from typing import Any, Dict, List, Optional

from ..models.pipeline import PipelineDefinition, StageConfig, PipelineStage
from ..config import settings
from ..utils.errors import ConfigurationError


def load_pipeline_definition(name: str, config_dir: Optional[str] = None) -> PipelineDefinition:
    config_path = config_dir or settings.pipeline_config_path
    path = os.path.join(config_path, f"{name}.json")

    if not os.path.exists(path):
        raise ConfigurationError(f"Pipeline definition not found: {path}")

    with open(path, "r") as f:
        data = json.load(f)

    return _parse_pipeline(data)


def load_all_pipeline_definitions(config_dir: Optional[str] = None) -> List[PipelineDefinition]:
    config_path = config_dir or settings.pipeline_config_path

    if not os.path.isdir(config_path):
        return []

    pipelines = []
    for entry in os.scandir(config_path):
        if entry.is_file() and entry.name.endswith(".json"):
            try:
                with open(entry.path, "r") as f:
                    data = json.load(f)
                pipelines.append(_parse_pipeline(data))
            except Exception as e:
                raise ConfigurationError(f"Failed to load pipeline '{entry.name}': {e}") from e

    return pipelines


def _parse_pipeline(data: Dict[str, Any]) -> PipelineDefinition:
    stages_data = data.get("stages", [])
    stages = []
    for s in stages_data:
        stage_enum = _parse_stage_enum(s.get("stage", "validate"))
        stages.append(
            StageConfig(
                name=s["name"],
                stage=stage_enum,
                processor_class=s.get("processor_class", ""),
                config=s.get("config", {}),
                depends_on=s.get("depends_on", []),
                timeout_seconds=s.get("timeout_seconds"),
                retry_max_attempts=s.get("retry_max_attempts", 1),
                retry_delay_seconds=s.get("retry_delay_seconds", 1.0),
                tags=s.get("tags", []),
            )
        )

    pipeline = PipelineDefinition(
        name=data["name"],
        version=data.get("version", "1.0.0"),
        description=data.get("description"),
        stages=stages,
        input_schema=data.get("input_schema"),
        output_schema=data.get("output_schema"),
        tags=data.get("tags", []),
        enabled=data.get("enabled", True),
    )

    dag_errors = pipeline.validate_dag()
    if dag_errors:
        raise ConfigurationError(f"Pipeline '{data['name']}' has DAG errors: {'; '.join(dag_errors)}")

    return pipeline


def _parse_stage_enum(value: str) -> PipelineStage:
    mapping = {
        "validate": PipelineStage.VALIDATE,
        "transform": PipelineStage.TRANSFORM,
        "enrich": PipelineStage.ENRICH,
        "analyze": PipelineStage.ANALYZE,
        "route": PipelineStage.ROUTE,
    }
    if value not in mapping:
        raise ConfigurationError(f"Unknown pipeline stage: '{value}'. Valid: {list(mapping.keys())}")
    return mapping[value]
