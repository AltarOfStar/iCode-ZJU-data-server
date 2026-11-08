"""端点契约：健康检查、认证、租户隔离、背压（server-acceptance §2 / §6 / §7 T16）。"""

from __future__ import annotations

import hashlib

import pytest

from icode_data_server.config import Principal

from .helpers import SESSION, batch, repository_payload, send_snapshot, snapshot_bytes, unit

ALL_KEYS = {"accepted", "duplicates", "rejected", "deferred", "retry_after_ms"}


async def test_healthz(client) -> None:
    response = await client.http.get("/healthz")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["repository_backend"] == "memory"
    assert body["object_store_backend"] == "local"


async def test_missing_token_is_401(client) -> None:
    response = await client.post_batch(b"", token=None)
    assert response.status_code == 401


async def test_bad_token_is_401(client) -> None:
    response = await client.post_batch(b"", token="nope")
    assert response.status_code == 401


async def test_manifest_requires_auth(client) -> None:
    response = await client.post_manifest(SESSION, {"snapshot": {}, "workspace": {}}, token=None)
    assert response.status_code == 401


async def test_batch_response_shape(client) -> None:
    response = await client.post_batch(b"")
    assert response.status_code == 200
    assert set(response.json()) == ALL_KEYS


async def test_client_reported_identity_is_ignored(client) -> None:
    response = await client.post_batch(
        unit("repo.identity", "repo:repo-identity-1", repository_payload()),
        headers={"X-Tenant": "attacker", "X-User-Id": "attacker"},
    )
    assert response.status_code == 200
    assert response.json()["accepted"], response.json()
    assert {tenant for tenant, _unit_id in client.repository.receipts} == {"dev"}
    assert await client.repository.list_repositories("attacker") == []
    rows = await client.repository.list_repositories("dev")
    assert rows[0].user_id == "dev-user"


async def test_tenant_isolation_for_objects_and_manifest(custom_client) -> None:
    client = await custom_client(
        tokens={
            "token-a": Principal(tenant="tenant-a", user_id="user-a"),
            "token-b": Principal(tenant="tenant-b", user_id="user-b"),
        }
    )
    content = snapshot_bytes(300 * 1024)
    content_hash, _chunks = await send_snapshot(client, content, token="token-a")

    body = {"snapshot": {"content_hash": content_hash, "chunk_hashes": []}, "workspace": {}}
    a = await client.post_manifest(SESSION, body, token="token-a")
    b = await client.post_manifest(SESSION, body, token="token-b")
    assert a.json()["known_snapshot"] is True
    assert b.json()["known_snapshot"] is False
    # 对象按租户分键：B 的租户路径下没有这个对象
    assert await client.object_store.exists("tenant-a", content_hash)
    assert not await client.object_store.exists("tenant-b", content_hash)


async def test_rate_limited_batch_returns_429(custom_client) -> None:
    client = await custom_client(rate_limit_per_second=0.0001, rate_limit_burst=1.0)
    frame = unit("repo.identity", "repo:repo-identity-1", repository_payload())
    first = await client.post_batch(frame)
    assert first.status_code == 200
    second = await client.post_batch(frame)
    assert second.status_code == 429
    assert second.headers["retry-after"]


async def test_per_user_rate_limit_does_not_block_other_users(custom_client) -> None:
    """单用户桶防单点刷量，但同一租户的其他用户不受影响（§6.4-2）。"""
    client = await custom_client(
        tokens={
            "token-a": Principal(tenant="acme", user_id="user-a"),
            "token-b": Principal(tenant="acme", user_id="user-b"),
        },
        rate_limit_per_second=100.0,
        rate_limit_burst=100.0,
        rate_limit_per_user_per_second=0.0001,
        rate_limit_per_user_burst=1.0,
    )
    frame = unit("repo.identity", "repo:repo-identity-1", repository_payload())
    assert (await client.post_batch(frame, token="token-a")).status_code == 200
    assert (await client.post_batch(frame, token="token-a")).status_code == 429
    assert (await client.post_batch(frame, token="token-b")).status_code == 200


async def test_overloaded_request_is_503_before_reading_the_body(custom_client) -> None:
    """请求闸门满时立即 503，不读体、不产生副作用（§6.4-1）。"""
    client = await custom_client(max_concurrent_requests=1)
    gate = client.services.gate
    assert gate.try_enter()
    try:
        frame = unit("repo.identity", "repo:repo-identity-1", repository_payload())
        response = await client.post_batch(frame)
        assert response.status_code == 503
        assert response.json()["error"] == "overloaded"
        assert response.headers["retry-after"]
        assert client.repository.receipts == {}
    finally:
        gate.leave()
    frame = unit("repo.identity", "repo:repo-identity-1", repository_payload())
    assert (await client.post_batch(frame)).status_code == 200


async def test_backpressure_defers_whole_batch(custom_client) -> None:
    client = await custom_client(max_inflight_batches_per_tenant=1)
    guard = client.services.processor.guard
    assert guard.enter("dev")
    try:
        frame = unit("repo.identity", "repo:repo-identity-1", repository_payload())
        response = await client.post_batch(frame)
        body = response.json()
        assert response.status_code == 200
        assert body["accepted"] == []
        assert len(body["deferred"]) == 1
        assert body["retry_after_ms"] > 0
        assert client.repository.receipts == {}
    finally:
        guard.leave("dev")


@pytest.mark.parametrize("scope", ["bogus-scope", "chunk:nothex:0"])
async def test_bad_chunk_scopes_are_schema_invalid(client, scope: str) -> None:
    response = (
        await client.post_batch(batch(unit("snapshot.chunk", scope, b"data", session_id=SESSION)))
    ).json()
    assert response["rejected"][0]["code"] == "schema_invalid"


async def test_repo_identity_scope_must_match(client) -> None:
    payload = repository_payload(identity="identity-x")
    response = (
        await client.post_batch(unit("repo.identity", "repo:other-identity", payload))
    ).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert client.repository.repositories == {}


async def test_repository_registry_row_is_written(client) -> None:
    payload = repository_payload()
    response = (
        await client.post_batch(unit("repo.identity", "repo:repo-identity-1", payload))
    ).json()
    assert response["accepted"], response
    rows = await client.repository.list_repositories("dev")
    assert len(rows) == 1
    assert rows[0].host == "github.com"
    assert rows[0].api_path == "o/r"
    assert rows[0].provider == "github"
    assert rows[0].user_id == "dev-user"
    assert rows[0].repository_key == hashlib.sha256(b"github.com/o/r").hexdigest()


async def test_unknown_kind_does_not_break_the_rest_of_the_batch(client) -> None:
    frames = batch(
        unit("control.watermark", "wm:1", b"{}"),
        unit("repo.identity", "repo:repo-identity-1", repository_payload()),
    )
    body = (await client.post_batch(frames)).json()
    assert len(body["rejected"]) == 1
    assert len(body["accepted"]) == 1
