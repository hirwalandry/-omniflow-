import io
import csv
import json
from typing import Any, Dict, List
from enum import Enum

import orjson
import yaml


class SerializationFormat(Enum):
    JSON = "json"
    YAML = "yaml"
    CSV = "csv"
    ORJSON = "orjson"


class Serializer:
    @staticmethod
    def serialize(data: Any, fmt: SerializationFormat = SerializationFormat.ORJSON) -> bytes:
        if fmt == SerializationFormat.JSON:
            return json.dumps(data, indent=2, default=str).encode("utf-8")
        if fmt == SerializationFormat.ORJSON:
            return orjson.dumps(data, option=orjson.OPT_INDENT_2 | orjson.OPT_SERIALIZE_NUMPY)
        if fmt == SerializationFormat.YAML:
            return yaml.dump(data, default_flow_style=False).encode("utf-8")
        if fmt == SerializationFormat.CSV:
            return Serializer._to_csv(data)
        raise ValueError(f"Unsupported format: {fmt}")

    @staticmethod
    def deserialize(data: bytes, fmt: SerializationFormat = SerializationFormat.ORJSON) -> Any:
        if fmt == SerializationFormat.ORJSON:
            return orjson.loads(data)
        if fmt == SerializationFormat.JSON:
            return json.loads(data)
        if fmt == SerializationFormat.YAML:
            return yaml.safe_load(data)
        if fmt == SerializationFormat.CSV:
            return Serializer._from_csv(data)
        raise ValueError(f"Unsupported format: {fmt}")

    @staticmethod
    def _to_csv(data: List[Dict[str, Any]]) -> bytes:
        if not data:
            return b""
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)
        return output.getvalue().encode("utf-8")

    @staticmethod
    def _from_csv(data: bytes) -> List[Dict[str, str]]:
        input_stream = io.StringIO(data.decode("utf-8"))
        reader = csv.DictReader(input_stream)
        return list(reader)
