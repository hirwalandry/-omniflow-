from __future__ import annotations
from typing import Any, Dict, List, Optional

from .base import Processor
from ..models.document import Document, ProcessingStage
from ..models.pipeline import StageConfig
from ..utils.errors import ValidationError


class SchemaValidator(Processor):
    def __init__(self, stage_config: StageConfig, metrics=None):
        super().__init__(stage_config, metrics)
        self.required_fields = stage_config.config.get("required_fields", [])
        self.field_constraints = stage_config.config.get("field_constraints", {})
        self.allowed_types = stage_config.config.get("allowed_types", {})

    @property
    def stage(self) -> ProcessingStage:
        return ProcessingStage.VALIDATION

    async def validate(self, document: Document) -> bool:
        return document.payload is not None and isinstance(document.payload, dict)

    async def process(self, document: Document) -> Document:
        errors = self._validate_payload(document.payload)
        if errors:
            raise ValidationError(f"Schema validation failed: {'; '.join(errors)}", {"field_errors": errors})
        return document

    def _validate_payload(self, payload: Dict[str, Any]) -> List[str]:
        errors = []

        for field in self.required_fields:
            if field not in payload:
                errors.append(f"Missing required field: '{field}'")

        for field, constraints in self.field_constraints.items():
            value = payload.get(field)
            if value is None:
                continue

            if "type" in constraints:
                expected = constraints["type"]
                if not isinstance(value, eval(expected)):
                    errors.append(f"Field '{field}' expected type {expected}, got {type(value).__name__}")

            if isinstance(value, str):
                if "min_length" in constraints and len(value) < constraints["min_length"]:
                    errors.append(f"Field '{field}' below minimum length {constraints['min_length']}")
                if "max_length" in constraints and len(value) > constraints["max_length"]:
                    errors.append(f"Field '{field}' exceeds maximum length {constraints['max_length']}")
                if "pattern" in constraints:
                    import re
                    if not re.match(constraints["pattern"], value):
                        errors.append(f"Field '{field}' does not match pattern {constraints['pattern']}")

            if isinstance(value, (int, float)):
                if "min" in constraints and value < constraints["min"]:
                    errors.append(f"Field '{field}' below minimum {constraints['min']}")
                if "max" in constraints and value > constraints["max"]:
                    errors.append(f"Field '{field}' exceeds maximum {constraints['max']}")

            if isinstance(value, list):
                if "min_items" in constraints and len(value) < constraints["min_items"]:
                    errors.append(f"Field '{field}' has fewer items than {constraints['min_items']}")
                if "max_items" in constraints and len(value) > constraints["max_items"]:
                    errors.append(f"Field '{field}' has more items than {constraints['max_items']}")
                if "unique_items" in constraints and constraints["unique_items"]:
                    if len(value) != len(set(value)):
                        errors.append(f"Field '{field}' contains duplicate items")

        return errors
