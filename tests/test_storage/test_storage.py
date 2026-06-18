from __future__ import annotations
import os

import pytest

from src.storage.console import ConsoleStorageHandler
from src.storage.file_storage import FileStorageHandler
from src.storage.database import DatabaseStorageHandler
from src.storage.base import BatchStorageEngine
from src.models.document import Document, DocumentMetadata


@pytest.fixture
def doc() -> Document:
    return Document(
        tenant_id="t1",
        pipeline_name="p1",
        metadata=DocumentMetadata(source_name="src", source_type="csv"),
        payload={"key": "value"},
    )


class TestConsoleStorage:
    async def test_store(self, doc, capsys):
        handler = ConsoleStorageHandler()
        result = await handler.store(doc)
        assert result.success
        assert result.document_id == doc.id
        captured = capsys.readouterr()
        assert doc.id in captured.out

    async def test_health(self):
        handler = ConsoleStorageHandler()
        assert await handler.health_check()


class TestFileStorage:
    async def test_store_json(self, tmp_path, doc):
        output = os.path.join(tmp_path, "files")
        handler = FileStorageHandler("fs", output_dir=output)
        result = await handler.store(doc)
        assert result.success
        assert os.path.exists(result.metadata["path"])
        assert result.metadata["path"].endswith(".json")

    async def test_store_with_sidecar(self, tmp_path, doc):
        output = os.path.join(tmp_path, "files")
        handler = FileStorageHandler("fs", output_dir=output, config={"create_sidecar": True})
        await handler.store(doc)
        meta_path = os.path.join(output, f"{doc.id}.meta.json")
        assert os.path.exists(meta_path)

    async def test_store_without_sidecar(self, tmp_path, doc):
        output = os.path.join(tmp_path, "files")
        handler = FileStorageHandler("fs", output_dir=output, config={"create_sidecar": False})
        await handler.store(doc)
        meta_path = os.path.join(output, f"{doc.id}.meta.json")
        assert not os.path.exists(meta_path)

    async def test_health(self, tmp_path):
        handler = FileStorageHandler("fs", output_dir=os.path.join(tmp_path, "out"))
        assert await handler.health_check()

    async def test_multi_store(self, tmp_path, doc):
        output = os.path.join(tmp_path, "files")
        handler = FileStorageHandler("fs", output_dir=output, config={"create_sidecar": False})
        for i in range(3):
            d = Document(
                tenant_id="t1", pipeline_name="p1",
                metadata=DocumentMetadata(source_name="src", source_type="csv"),
                payload={"idx": i},
            )
            r = await handler.store(d)
            assert r.success
        files = os.listdir(output)
        json_files = [f for f in files if f.endswith(".json")]
        assert len(json_files) == 3


class TestDatabaseStorage:
    async def test_store_and_retrieve(self, doc):
        db = DatabaseStorageHandler("db")
        result = await db.store(doc)
        assert result.success

        retrieved = await db.get_document(doc.id)
        assert retrieved is not None
        assert retrieved.payload["key"] == "value"

    async def test_store_and_get_record(self, doc):
        db = DatabaseStorageHandler("db")
        await db.store(doc)
        record = await db.get_record(doc.id)
        assert record is not None
        assert record["payload"]["key"] == "value"
        assert record["status"] == "pending"

    async def test_delete(self, doc):
        db = DatabaseStorageHandler("db")
        await db.store(doc)
        assert await db.get_document(doc.id) is not None
        assert await db.delete_document(doc.id) is True
        assert await db.get_document(doc.id) is None

    async def test_query_filter_by_status(self, doc):
        db = DatabaseStorageHandler("db")
        await db.store(doc)
        from src.models.document import DocumentStatus
        results = await db.query(status=DocumentStatus.PENDING)
        assert len(results) >= 1

    async def test_query_filter_by_pipeline(self, doc):
        db = DatabaseStorageHandler("db")
        await db.store(doc)
        results = await db.query(pipeline="p1")
        assert len(results) >= 1
        results2 = await db.query(pipeline="nonexistent")
        assert len(results2) == 0

    async def test_query_limit(self, doc):
        db = DatabaseStorageHandler("db")
        for i in range(10):
            d = Document(
                tenant_id="t1", pipeline_name="p1",
                metadata=DocumentMetadata(source_name="src", source_type="csv"),
                payload={"idx": i},
            )
            await db.store(d)
        results = await db.query(limit=5)
        assert len(results) == 5

    async def test_health(self):
        db = DatabaseStorageHandler()
        assert await db.health_check()


class TestBatchStorageEngine:
    async def test_store_all_success(self, doc):
        handlers = [ConsoleStorageHandler("console"), DatabaseStorageHandler("db")]
        engine = BatchStorageEngine(handlers)
        results = await engine.store_all(doc)
        assert len(results) == 2
        assert all(r.success for r in results)
        assert results[0].destination == "console"
        assert results[1].destination == "in_memory_db"
