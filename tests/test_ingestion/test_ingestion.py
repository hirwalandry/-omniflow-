from __future__ import annotations
import json
import os

import pytest

from src.ingestion.file_source import FileSourceHandler
from src.ingestion.base import BatchIngestionEngine


class TestFileSource:
    async def test_discover_empty_dir(self, tmp_path):
        handler = FileSourceHandler("src", watch_dir=str(tmp_path))
        ids = await handler.discover()
        assert ids == []

    async def test_discover_files(self, tmp_path):
        for fname in ["a.json", "b.yaml", "c.yml", "d.csv", "e.txt"]:
            (tmp_path / fname).write_text("{}" if fname.endswith(".json") else "key: value")

        handler = FileSourceHandler("src", watch_dir=str(tmp_path))
        ids = await handler.discover()
        assert len(ids) == 4

    async def test_fetch_json(self, tmp_path):
        payload = {"name": "Alice", "role": "engineer"}
        filepath = tmp_path / "alice.json"
        filepath.write_text(json.dumps(payload))

        handler = FileSourceHandler("src", watch_dir=str(tmp_path))
        doc = await handler.fetch(str(filepath))
        assert doc.payload["name"] == "Alice"
        assert doc.metadata.source_type == "file"
        assert doc.metadata.checksum is not None

    async def test_acknowledge_tracking(self, tmp_path):
        (tmp_path / "doc.json").write_text("{}")
        handler = FileSourceHandler("src", watch_dir=str(tmp_path))
        ids = await handler.discover()
        assert len(ids) == 1

        await handler.acknowledge(ids[0], True)
        ids2 = await handler.discover()
        assert len(ids2) == 0

    async def test_health_check(self, tmp_path):
        handler = FileSourceHandler("src", watch_dir=str(tmp_path))
        assert await handler.health_check() is True

        broken = FileSourceHandler("src", watch_dir=r"\nonexistent\path")
        assert await broken.health_check() is False


class TestBatchIngestionEngine:
    async def test_ingest_all(self, tmp_path):
        for name in ["a.json", "b.json"]:
            (tmp_path / name).write_text(json.dumps({"file": name}))

        handler = FileSourceHandler("files", watch_dir=str(tmp_path))
        engine = BatchIngestionEngine([handler])
        docs = await engine.ingest_all()
        assert len(docs) == 2
        for doc in docs:
            assert doc.status.value == "ingested"
            assert len(doc.processing_history) == 1
            assert doc.processing_history[0].stage.value == "ingestion"
