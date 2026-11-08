"""测试夹具：全部走内存仓储 + 本地对象存储 + 临时目录，禁止外网。"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from icode_data_server.app import create_app
from icode_data_server.config import Principal, Settings

DEV_TOKEN = "dev-token"
TENANT = "dev"
USER = "dev-user"


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass
class Client:
    http: AsyncClient
    app: Any
    settings: Settings
    staging: Path
    objects: Path

    @property
    def services(self):
        return self.app.state.services

    @property
    def repository(self):
        return self.services.repository

    @property
    def object_store(self):
        return self.services.objects

    async def post_batch(
        self,
        body: bytes,
        *,
        token: str | None = DEV_TOKEN,
        headers: dict[str, str] | None = None,
    ):
        request_headers = {"Content-Type": "application/vnd.chrys.report.frames"}
        if token is not None:
            request_headers["Authorization"] = f"Bearer {token}"
        if headers:
            request_headers.update(headers)
        return await self.http.post("/v1/batch", content=body, headers=request_headers)

    async def post_manifest(self, session_id: str, body: dict, *, token: str | None = DEV_TOKEN):
        headers = {"Content-Type": "application/json"}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        return await self.http.post(
            f"/v1/session/{session_id}/manifest", json=body, headers=headers
        )


def make_settings(tmp_path: Path, **overrides: Any) -> Settings:
    base = Settings(
        tokens={DEV_TOKEN: Principal(tenant=TENANT, user_id=USER)},
        repository_backend="memory",
        object_store_backend="local",
        local_object_root=tmp_path / "objects",
        staging_root=tmp_path / "staging",
        rate_limit_per_second=0.0,
        rate_limit_burst=1000.0,
        rate_limit_per_user_per_second=0.0,
        rate_limit_per_user_burst=1000.0,
        assembly_sweep_interval_s=0.0,
    )
    return replace(base, **overrides) if overrides else base


@pytest_asyncio.fixture
async def client(tmp_path: Path) -> AsyncIterator[Client]:
    settings = make_settings(tmp_path)
    app = create_app(settings)
    services = app.state.services
    await services.start()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http:
        yield Client(
            http=http,
            app=app,
            settings=settings,
            staging=settings.staging_root,
            objects=settings.local_object_root,
        )
    await services.close()


def client_with(tmp_path: Path, **overrides: Any) -> Client:
    """同步构造（供需要自定义限额的用例在 async fixture 外使用）。"""
    settings = make_settings(tmp_path, **overrides)
    app = create_app(settings)
    return Client(
        http=None,  # type: ignore[arg-type]
        app=app,
        settings=settings,
        staging=settings.staging_root,
        objects=settings.local_object_root,
    )


@pytest_asyncio.fixture
async def custom_client(tmp_path: Path):
    """返回工厂：`await factory(rate_limit_per_second=..., ...)`。"""

    created: list[tuple[Any, AsyncClient]] = []

    async def factory(**overrides: Any) -> Client:
        settings = make_settings(tmp_path, **overrides)
        app = create_app(settings)
        await app.state.services.start()
        transport = ASGITransport(app=app)
        http = AsyncClient(transport=transport, base_url="http://test")
        created.append((app, http))
        return Client(
            http=http,
            app=app,
            settings=settings,
            staging=settings.staging_root,
            objects=settings.local_object_root,
        )

    try:
        yield factory
    finally:
        for app, http in created:
            await http.aclose()
            await app.state.services.close()


@pytest.fixture
def event_line():
    def build(
        sequence: int,
        event_type: str,
        *,
        branch_id: str | None = "b1",
        payload: dict | None = None,
        turn_id: str | None = "t1",
        occurred_at: str = "2026-09-09T08:00:00Z",
        event_id: str | None = None,
        actor: dict | None = None,
    ) -> bytes:
        document: dict[str, Any] = {
            "schema_version": 1,
            "event_id": event_id or f"e{sequence}",
            "sequence": sequence,
            "event_type": event_type,
            "occurred_at": occurred_at,
            "monotonic_ns": sequence * 1000,
            "actor": actor or {"kind": "agent", "role": "main"},
            "payload": payload or {},
        }
        if branch_id is not None:
            document["branch_id"] = branch_id
        if turn_id is not None:
            document["turn_id"] = turn_id
        return json.dumps(document, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    return build


@pytest.fixture
def payload_file(tmp_path: Path):
    counter = {"n": 0}

    def write(data: bytes) -> Path:
        counter["n"] += 1
        path = tmp_path / f"payload-{counter['n']}.bin"
        path.write_bytes(data)
        return path

    return write
