from __future__ import annotations
import re
from typing import Any, Callable, Dict, List, Optional

from .base import Processor
from ..models.document import Document, DocumentStatus, ProcessingStage, RoutingDirective
from ..models.pipeline import StageConfig
from ..utils.errors import RoutingError


class DocumentRouter(Processor):
    def __init__(self, stage_config: StageConfig, metrics=None):
        super().__init__(stage_config, metrics)
        self.routes = stage_config.config.get("routes", [])
        self.fallback_destination = stage_config.config.get("fallback_destination", "default")
        self.default_priority = stage_config.config.get("default_priority", 0)

    @property
    def stage(self) -> ProcessingStage:
        return ProcessingStage.ROUTING

    async def validate(self, document: Document) -> bool:
        return document.status in (DocumentStatus.ENRICHED, DocumentStatus.TRANSFORMED, DocumentStatus.ANALYZED)

    async def process(self, document: Document) -> Document:
        payload = document.transformed_payload or document.payload
        if not isinstance(payload, dict):
            raise RoutingError("Cannot route document without a dict payload")

        for route in self.routes:
            route_type = route.get("type", "metadata")
            condition = route.get("condition", {})
            destination = route.get("destination")
            priority = route.get("priority", self.default_priority)
            ttl = route.get("ttl_seconds")
            transform = route.get("transform_before_delivery")

            if not destination:
                continue

            if self._evaluate_condition(route_type, condition, document, payload):
                document.routing = RoutingDirective(
                    destination=destination,
                    priority=priority,
                    ttl_seconds=ttl,
                    transform_before_delivery=transform,
                    delivery_ack_required=route.get("delivery_ack_required", False),
                )
                return document

        document.routing = RoutingDirective(
            destination=self.fallback_destination,
            priority=self.default_priority,
        )
        return document

    def _evaluate_condition(
        self, route_type: str, condition: Dict[str, Any], document: Document, payload: Dict[str, Any]
    ) -> bool:
        if route_type == "tenant_based":
            return self._match_field(condition, document.tenant_id)

        elif route_type == "metadata_based":
            field = condition.get("field", "")
            expected = condition.get("value")
            actual = self._get_nested(document.metadata.custom, field)
            return self._compare(actual, expected, condition.get("operator", "equals"))

        elif route_type == "payload_based":
            field = condition.get("field", "")
            expected = condition.get("value")
            actual = self._get_nested(payload, field)
            return self._compare(actual, expected, condition.get("operator", "equals"))

        elif route_type == "status_based":
            return document.status.value == condition.get("status")

        elif route_type == "tag_based":
            required_tags = condition.get("tags", [])
            doc_tags = set(t.lower() for t in document.metadata.tags)
            if condition.get("match_all", False):
                return all(t.lower() in doc_tags for t in required_tags)
            return any(t.lower() in doc_tags for t in required_tags)

        elif route_type == "pattern_based":
            field = condition.get("field", "")
            pattern = condition.get("pattern", "")
            value = str(self._get_nested(payload, field) or "")
            return bool(re.search(pattern, value))

        return False

    def _match_field(self, condition: Dict[str, Any], actual: str) -> bool:
        expected = condition.get("value")
        operator = condition.get("operator", "equals")

        if operator == "equals":
            return actual == expected
        elif operator == "in":
            return actual in condition.get("values", [])
        elif operator == "not_in":
            return actual not in condition.get("values", [])
        elif operator == "pattern":
            return bool(re.search(condition.get("pattern", ""), actual))
        return False

    def _compare(self, actual: Any, expected: Any, operator: str) -> bool:
        if operator == "equals":
            return actual == expected
        elif operator == "not_equals":
            return actual != expected
        elif operator == "contains":
            return expected in actual if isinstance(actual, (str, list)) else False
        elif operator == "gt":
            return isinstance(actual, (int, float)) and actual > expected
        elif operator == "gte":
            return isinstance(actual, (int, float)) and actual >= expected
        elif operator == "lt":
            return isinstance(actual, (int, float)) and actual < expected
        elif operator == "lte":
            return isinstance(actual, (int, float)) and actual <= expected
        elif operator == "exists":
            return actual is not None
        elif operator == "not_exists":
            return actual is None
        return False

    def _get_nested(self, data: Dict[str, Any], path: str) -> Any:
        parts = path.split(".")
        current = data
        for part in reversed(parts):
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current
