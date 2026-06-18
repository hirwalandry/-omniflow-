from __future__ import annotations
import json
import re
from typing import Any, Dict, List, Optional, Set
from datetime import datetime, timezone

from .base import Processor
from ..models.document import Document, ProcessingStage
from ..models.pipeline import StageConfig
from ..utils.errors import EnrichmentError


ENTITY_PATTERNS: Dict[str, str] = {
    "email": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    "phone": r"\+?\d[\d\s\-().]{6,}\d",
    "url": r"https?://[^\s<>\"']+|www\.[^\s<>\"']+",
    "date_iso": r"\d{4}-\d{2}-\d{2}",
    "ipv4": r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
}


class DataEnricher(Processor):
    def __init__(self, stage_config: StageConfig, metrics=None):
        super().__init__(stage_config, metrics)
        self.dedup_fields = stage_config.config.get("dedup_fields", [])
        self.extract_entities = stage_config.config.get("extract_entities", False)
        self.entity_patterns = {
            k: re.compile(v)
            for k, v in ENTITY_PATTERNS.items()
            if k in stage_config.config.get("entity_types", list(ENTITY_PATTERNS.keys()))
        }
        self.tag_sources = stage_config.config.get("tag_sources", [])
        self.computed_tags = stage_config.config.get("computed_tags", [])
        self._seen_values: Dict[str, Set[str]] = {}

    @property
    def stage(self) -> ProcessingStage:
        return ProcessingStage.ENRICHMENT

    async def validate(self, document: Document) -> bool:
        payload = document.transformed_payload or document.payload
        return payload is not None and isinstance(payload, dict)

    async def process(self, document: Document) -> Document:
        payload = document.transformed_payload or document.payload
        if not isinstance(payload, dict):
            raise EnrichmentError("Enrichment requires a dict payload")

        enriched: Dict[str, Any] = {"_dedup": {}, "_entities": {}, "_tags": []}

        if self.dedup_fields:
            enriched["_dedup"] = self._check_duplicates(document, payload)

        if self.entity_patterns:
            enriched["_entities"] = self._extract_entities(payload)

        if self.tag_sources or self.computed_tags:
            enriched["_tags"] = self._compute_tags(document, payload)

        if self.extract_entities and "entities" not in enriched["_entities"]:
            pass

        document.enriched_payload = enriched

        if enriched["_tags"]:
            existing = set(document.metadata.tags)
            document.metadata.tags = list(existing | set(enriched["_tags"]))

        return document

    def _check_duplicates(self, document: Document, payload: Dict[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        is_dup = False

        for field in self.dedup_fields:
            value = payload.get(field)
            if value is None:
                continue

            key = str(value)
            seen = self._seen_values.setdefault(field, set())

            if key in seen:
                is_dup = True
                result[field] = {"duplicate": True, "value": value}
            else:
                seen.add(key)
                result[field] = {"duplicate": False, "value": value}

        result["is_duplicate"] = is_dup
        return result

    def _extract_entities(self, payload: Dict[str, Any]) -> Dict[str, List[str]]:
        found: Dict[str, List[str]] = {}
        text = json.dumps(payload) if isinstance(payload, dict) else str(payload)

        for entity_type, pattern in self.entity_patterns.items():
            matches = pattern.findall(text)
            if matches:
                found[entity_type] = list(set(matches))

        return found

    def _compute_tags(self, document: Document, payload: Dict[str, Any]) -> List[str]:
        tags: List[str] = []

        for source_field in self.tag_sources:
            value = payload.get(source_field)
            if isinstance(value, str) and value:
                tags.append(value.lower().replace(" ", "_"))
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        tags.append(item.lower().replace(" ", "_"))

        for tag_def in self.computed_tags:
            field = tag_def.get("field")
            condition = tag_def.get("condition", "exists")
            tag_value = tag_def.get("tag")

            if field and tag_value:
                val = payload.get(field)
                if condition == "exists" and val is not None:
                    tags.append(tag_value)
                elif condition == "equals" and val == tag_def.get("value"):
                    tags.append(tag_value)
                elif condition == "gt" and isinstance(val, (int, float)) and val > tag_def.get("value", 0):
                    tags.append(tag_value)
                elif condition == "lt" and isinstance(val, (int, float)) and val > tag_def.get("value", 0):
                    tags.append(tag_val)
                elif condition == "lt" and isinstance(val, (int, float)) and val < tag_def.get("value", 0):
                    tags.append(tag_value)
