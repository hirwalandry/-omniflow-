from __future__ import annotations
import json
from typing import Any, Dict, List, Optional

from .base import Processor
from ..models.document import Document, ProcessingStage
from ..models.pipeline import StageConfig
from ..utils.errors import TransformationError


class DataTransformer(Processor):
    def __init__(self, stage_config: StageConfig, metrics=None):
        super().__init__(stage_config, metrics)
        self.operations = stage_config.config.get("operations", [])
        self.schema_version = stage_config.config.get("schema_version", "1.0")

    @property
    def stage(self) -> ProcessingStage:
        return ProcessingStage.TRANSFORMATION

    async def validate(self, document: Document) -> bool:
        return document.payload is not None

    async def process(self, document: Document) -> Document:
        payload = document.payload
        warnings = []

        for op in self.operations:
            op_type = op.get("type")
            try:
                if op_type == "rename_field":
                    payload = self._rename_field(payload, op["from"], op["to"])
                elif op_type == "remove_field":
                    payload = self._remove_field(payload, op["field"])
                elif op_type == "coerce_type":
                    payload = self._coerce_field(payload, op["field"], op["target_type"])
                elif op_type == "default_value":
                    payload = self._apply_default(payload, op["field"], op["default"])
                elif op_type == "map_values":
                    payload = self._map_values(payload, op["field"], op["mapping"])
                elif op_type == "flatten":
                    payload = self._flatten(payload, op.get("max_depth", 1))
                elif op_type == "aggregate":
                    warnings.append(f"Aggregation operation not yet implemented: {op.get('name', 'unnamed')}")
                elif op_type == "compute_field":
                    payload = self._compute_field(payload, op["target_field"], op.get("expression", ""))
                else:
                    warnings.append(f"Unknown operation type: {op_type}")
            except Exception as e:
                raise TransformationError(f"Operation '{op_type}' failed: {e}", {"operation": op, "error": str(e)})

        document.transformed_payload = payload
        if warnings:
            document.metadata.custom["transform_warnings"] = warnings
        return document

    def _rename_field(self, data: Dict[str, Any], old_name: str, new_name: str) -> Dict[str, Any]:
        if old_name in data:
            data[new_name] = data.pop(old_name)
        return data

    def _remove_field(self, data: Dict[str, Any], field: str) -> Dict[str, Any]:
        data.pop(field, None)
        return data

    def _coerce_field(self, data: Dict[str, Any], field: str, target_type: str) -> Dict[str, Any]:
        if field not in data:
            return data
        value = data[field]
        try:
            if target_type == "str":
                data[field] = str(value)
            elif target_type == "int":
                data[field] = int(value)
            elif target_type == "float":
                data[field] = float(value)
            elif target_type == "bool":
                if isinstance(value, str):
                    data[field] = value.lower() in ("true", "1", "yes", "y")
                else:
                    data[field] = bool(value)
            elif target_type == "list":
                if isinstance(value, str):
                    data[field] = json.loads(value)
                else:
                    data[field] = list(value)
            elif target_type == "dict":
                if isinstance(value, str):
                    data[field] = json.loads(value)
            return data
        except (ValueError, TypeError, json.JSONDecodeError) as e:
            raise TransformationError(f"Cannot coerce field '{field}' to {target_type}: {e}", {"field": field, "value": value, "target_type": target_type})

    def _apply_default(self, data: Dict[str, Any], field: str, default: Any) -> Dict[str, Any]:
        if field not in data or data[field] is None:
            data[field] = default
        return data

    def _map_values(self, data: Dict[str, Any], field: str, mapping: Dict[str, Any]) -> Dict[str, Any]:
        if field in data and data[field] in mapping:
            data[field] = mapping[data[field]]
        return data

    def _flatten(self, data: Dict[str, Any], max_depth: int, prefix: str = "") -> Dict[str, Any]:
        result = {}
        for key, value in data.items():
            new_key = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict) and max_depth > 0:
                result.update(self._flatten(value, max_depth - 1, new_key))
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        result.update(self._flatten(item, max_depth - 1, f"{new_key}[{i}]"))
                    else:
                        result[f"{new_key}[{i}]"] = item
            else:
                result[new_key] = value
        return result

    def _compute_field(self, data: Dict[str, Any], target_field: str, expression: str) -> Dict[str, Any]:
        safe_globals = {"__builtins__": {}}
        safe_locals = {**data, "len": len, "str": str, "int": int, "float": float, "bool": bool, "list": list, "dict": dict, "sum": sum, "min": min, "max": max, "abs": abs, "round": round}
        try:
            result = eval(expression, safe_globals, safe_locals)
            data[target_field] = result
        except Exception as e:
            raise TransformationError(f"Compute field '{target_field}' expression failed: {e}", {"expression": expression})
        return data
