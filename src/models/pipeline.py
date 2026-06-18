from __future__ import annotations
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4

from pydantic import BaseModel, Field


class PipelineStage(Enum):
    VALIDATE = "validate"
    TRANSFORM = "transform"
    ENRICH = "enrich"
    ANALYZE = "analyze"
    ROUTE = "route"


class StageConfig(BaseModel):
    name: str
    stage: PipelineStage
    processor_class: str = ""
    config: Dict[str, Any] = Field(default_factory=dict)
    depends_on: List[str] = Field(default_factory=list)
    timeout_seconds: Optional[int] = None
    retry_max_attempts: int = 1
    retry_delay_seconds: float = 1.0
    tags: List[str] = Field(default_factory=list)


class PipelineDefinition(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    version: str = "1.0.0"
    description: Optional[str] = None
    stages: List[StageConfig] = Field(default_factory=list)
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    tags: List[str] = Field(default_factory=list)
    enabled: bool = True

    def validate_dag(self) -> List[str]:
        errors = []
        stage_names = {s.name for s in self.stages}

        for stage in self.stages:
            for dep in stage.depends_on:
                if dep not in stage_names:
                    errors.append(f"Stage '{stage.name}' depends on unknown stage '{dep}'")
                elif dep == stage.name:
                    errors.append(f"Stage '{stage.name}' depends on itself")

        if self._has_cycle():
            errors.append("Pipeline contains circular dependency cycle")

        return errors

    def _has_cycle(self) -> bool:
        adj: Dict[str, List[str]] = {}
        for s in self.stages:
            adj.setdefault(s.name, [])
            for dep in s.depends_on:
                adj.setdefault(dep, [])
                adj[s.name].append(dep)

        visited: Set[str] = set()
        rec_stack: Set[str] = set()

        def dfs(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)
            for neighbor in adj.get(node, []):
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
            rec_stack.discard(node)
            return False

        return any(node not in visited and dfs(node) for node in adj)

    def topological_sort(self) -> List[StageConfig]:
        errors = self.validate_dag()
        if errors:
            raise ValueError(f"Cannot sort pipeline with errors: {errors}")

        visited: Set[str] = set()
        result: List[StageConfig] = []
        stage_map = {s.name: s for s in self.stages}

        def visit(name: str):
            if name in visited:
                return
            visited.add(name)
            stage = stage_map.get(name)
            if stage:
                for dep in stage.depends_on:
                    visit(dep)
                result.append(stage)

        for stage in self.stages:
            visit(stage.name)

        return result
