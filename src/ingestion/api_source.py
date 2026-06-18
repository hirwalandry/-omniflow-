from __future__ import annotations
import hashlib
from typing import Any, AsyncIterator, Dict, List, Optional
from datetime import datetime, timezone

import httpx

from .base import SourceHandler
from ..models.document import Document, DocumentMetadata


class APISourceHandler(SourceHandler):
    def __init__(self, source_name: str, endpoint_url: str, api_key: Optional[str] = None, config: Optional[dict] = None):
        super().__init__(source_name, config)
        self.endpoint_url = endpoint_url
        self.api_key = api_key
        self._client: Optional[httpx.AsyncClient] = None
        self._seen_ids: set = set()

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            headers = {"User-Agent": "OmniFlow/1.0"}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            self._client = httpx.AsyncClient(headers=headers, timeout=30.0)
        return self._client

    async def discover(self) -> List[str]:
        client = await self._get_client()
        resp = await client.get(f"{self.endpoint_url}/items", params={"limit": 100, "status": "pending"})
        resp.raise_for_status()
        data = resp.json()
        items = data if isinstance(data, list) else data.get("items", data.get("data", []))
        return [item["id"] for item in items if item["id"] not in self._seen_ids]

    async def fetch(self, source_id: str) -> Document:
        client = await self._get_client()
        resp = await client.get(f"{self.endpoint_url}/items/{source_id}")
        resp.raise_for_status()
        raw = resp.json()
        item = raw if isinstance(raw, dict) and "id" in raw else raw.get("item", raw.get("data", {}))

        raw_bytes = str(item).encode("utf-8")
        checksum = hashlib.sha256(raw_bytes).hexdigest()

        metadata = DocumentMetadata(
            source_name=self.source_name,
            source_type="api",
            content_type="application/json",
            content_length=len(raw_bytes),
            checksum=checksum,
            tags=["api-ingest"],
            custom={"endpoint": self.endpoint_url, "source_id": source_id},
        )

        tenant = self.config.get("tenant_id", "default")
        pipeline = self.config.get("pipeline", "default-pipeline")

        return Document(
            id=source_id,
            tenant_id=tenant,
            pipeline_name=pipeline,
            metadata=metadata,
            payload=item,
        )

    async def stream(self) -> AsyncIterator[Document]:
        ids = await self.discover()
        for sid in ids:
            doc = await self.fetch(sid)
            self._seen_ids.add(sid)
            yield doc

    async def acknowledge(self, source_id: str, success: bool) -> bool:
        client = await self._get_client()
        status = "processed" if success else "failed"
        resp = await client.patch(f"{self.endpoint_url}/items/{source_id}", json={"status": status})
        return resp.is_success

    async def health_check(self) -> bool:
        try:
            client = await self._get_client()
            resp = await client.get(f"{self.endpoint_url}/health", timeout=5.0)
            return resp.is_success
        except Exception:
            return False

    async def close(self):
        if self._client:
            await self._client.aclose()
