from __future__ import annotations
import json

import pytest

from src.utils.serialization import SerializationFormat, Serializer


class TestSerializer:
    def test_serialize_json(self):
        data = {"name": "Alice", "age": 30}
        result = Serializer.serialize(data, SerializationFormat.JSON)
        assert isinstance(result, bytes)
        parsed = json.loads(result)
        assert parsed["name"] == "Alice"

    def test_deserialize_json(self):
        raw = b'{"name": "Alice"}'
        result = Serializer.deserialize(raw, SerializationFormat.JSON)
        assert result["name"] == "Alice"

    def test_serialize_yaml(self):
        data = {"name": "Alice"}
        result = Serializer.serialize(data, SerializationFormat.YAML)
        assert isinstance(result, bytes)
        assert b"Alice" in result

    def test_deserialize_yaml(self):
        raw = b"name: Alice\n"
        result = Serializer.deserialize(raw, SerializationFormat.YAML)
        assert result["name"] == "Alice"

    def test_serialize_csv(self):
        data = [{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}]
        result = Serializer.serialize(data, SerializationFormat.CSV)
        assert isinstance(result, bytes)
        decoded = result.decode("utf-8")
        assert "Alice" in decoded
        assert "Bob" in decoded

    def test_deserialize_csv(self):
        raw = b"name,age\nAlice,30\nBob,25\n"
        result = Serializer.deserialize(raw, SerializationFormat.CSV)
        assert len(result) == 2
        assert result[0]["name"] == "Alice"
        assert result[1]["age"] == "25"

    def test_csv_roundtrip(self):
        data = [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]
        serialized = Serializer.serialize(data, SerializationFormat.CSV)
        deserialized = Serializer.deserialize(serialized, SerializationFormat.CSV)
        assert deserialized == data

    def test_orjson_default(self):
        data = {"name": "Alice"}
        result = Serializer.serialize(data, SerializationFormat.ORJSON)
        assert isinstance(result, bytes)
        assert b"Alice" in result

    def test_empty_csv(self):
        result = Serializer.serialize([], SerializationFormat.CSV)
        assert result == b""

    def test_invalid_format(self):
        with pytest.raises(ValueError, match="Unsupported format"):
            Serializer.serialize({"a": 1}, None)
