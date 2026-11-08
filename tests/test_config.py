"""配置与对象键布局（server-acceptance §4.1 / §6）。"""

from __future__ import annotations

import hashlib
from pathlib import Path

from icode_data_server.config import Settings
from icode_data_server.storage import build_object_store, build_repository
from icode_data_server.storage.objects import (
    LocalObjectStore,
    bare_etag,
    content_etag,
    object_key,
)
from icode_data_server.storage.repository import MemoryRepository


def test_defaults_are_zero_dependency(monkeypatch) -> None:
    for name in (
        "REPOSITORY_BACKEND",
        "OBJECT_STORE_BACKEND",
        "ICODE_DS_TOKENS",
        "ICODE_DS_PORT",
        "ICODE_DS_AUTH_DISABLED",
    ):
        monkeypatch.delenv(name, raising=False)
    settings = Settings.from_env()
    assert settings.repository_backend == "memory"
    assert settings.object_store_backend == "local"
    assert settings.port == 8080
    assert settings.max_body_bytes == 16 * 1024 * 1024
    assert settings.max_expanded_bytes == 16 * 1024 * 1024
    assert settings.max_frame_payload_bytes == 1024 * 1024
    assert settings.max_frames_per_batch == 4096
    assert settings.batch_timeout_s == 8.0
    assert settings.max_concurrent_requests == 64
    assert settings.rate_limit_per_second == 50.0
    assert settings.rate_limit_per_user_per_second == 5.0
    assert settings.max_inflight_batches_per_tenant == 64
    assert settings.staging_bytes_per_tenant == 4 * 1024 * 1024 * 1024
    assert settings.postgres_pool_max_size == 20
    assert settings.max_window_index_lines == 200
    assert settings.max_window_bytes == 4 * 1024 * 1024
    assert settings.assembly_ttl_s == 24 * 3600
    assert settings.agent_env_max_bytes == 256 * 1024
    assert settings.principal_for_token("dev-token") is not None
    assert build_repository(settings).__class__ is MemoryRepository
    assert build_object_store(settings).__class__ is LocalObjectStore


def test_env_overrides(monkeypatch) -> None:
    monkeypatch.setenv("REPOSITORY_BACKEND", "postgres")
    monkeypatch.setenv("OBJECT_STORE_BACKEND", "s3")
    monkeypatch.setenv("ICODE_DS_S3_BUCKET", "bucket")
    monkeypatch.setenv("ICODE_DS_TOKENS", '{"t1": {"tenant": "acme", "user_id": "u1"}}')
    monkeypatch.setenv("ICODE_DS_PORT", "9000")
    monkeypatch.setenv("ICODE_DS_MAX_CONCURRENT_REQUESTS", "16")
    monkeypatch.setenv("ICODE_DS_RATE_LIMIT_PER_USER_PER_SECOND", "2.5")
    monkeypatch.setenv("ICODE_DS_POSTGRES_POOL_MAX_SIZE", "40")
    monkeypatch.setenv("ICODE_DS_POSTGRES_POOL_TIMEOUT_S", "2.5")
    settings = Settings.from_env()
    assert settings.repository_backend == "postgres"
    assert settings.object_store_backend == "s3"
    assert settings.s3_bucket == "bucket"
    assert settings.port == 9000
    assert settings.max_concurrent_requests == 16
    assert settings.rate_limit_per_user_per_second == 2.5
    assert settings.postgres_pool_max_size == 40
    assert settings.postgres_pool_timeout_s == 2.5
    principal = settings.principal_for_token("t1")
    assert principal is not None
    assert (principal.tenant, principal.user_id) == ("acme", "u1")
    assert settings.principal_for_token("dev-token") is None


def test_object_key_layout() -> None:
    sha = "abcdef" + "0" * 58
    assert object_key("acme", sha) == f"t/acme/o/ab/{sha}"


async def test_local_object_store_round_trip(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path)
    await store.start()
    etag = await store.put("acme", "a" * 64, b"payload")
    assert await store.get("acme", "a" * 64) == b"payload"
    # 登记锚点用单段上传的 ETag：本地实现与 OBS 一样给出内容摘要。
    assert etag == content_etag(b"payload")
    assert etag == hashlib.md5(b"payload", usedforsecurity=False).hexdigest()
    assert await store.exists("acme", "a" * 64)
    assert not await store.exists("other", "a" * 64)


def test_provider_etags_lose_their_quotes() -> None:
    assert bare_etag('"abc123"') == "abc123"
    assert bare_etag("  abc123  ") == "abc123"
    assert bare_etag('""') is None
    assert bare_etag(None) is None
