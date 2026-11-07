"""`agent.env` 存取与 `agent-config/1.0` 校验（server-acceptance §3.2-7 / §7 T22）。"""

from __future__ import annotations

import json

from .helpers import (
    SESSION,
    agent_env_payload,
    send_snapshot,
    session_meta_payload,
    sha,
    snapshot_bytes,
    unit,
)


async def test_agent_env_is_content_addressed_and_anchored(client) -> None:
    payload = agent_env_payload()
    digest = sha(payload)
    response = (
        await client.post_batch(
            unit("agent.env", f"agentenv:{digest}", payload, session_id=SESSION)
        )
    ).json()
    assert response["accepted"], response

    assert await client.object_store.get("dev", digest) == payload
    record = await client.repository.get_object("dev", digest)
    assert record is not None and record.state == "verified"
    assert record.channel == "agent-config"
    assert await client.repository.get_agent_config_anchor("dev", SESSION) == digest


async def test_agent_env_missing_redacted_is_rejected(client) -> None:
    document = json.loads(agent_env_payload())
    document.pop("redacted")
    payload = json.dumps(document).encode()
    response = (
        await client.post_batch(
            unit("agent.env", f"agentenv:{sha(payload)}", payload, session_id=SESSION)
        )
    ).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert "redacted" in response["rejected"][0]["message"]
    assert not await client.object_store.exists("dev", sha(payload))


async def test_agent_env_wrong_schema_is_rejected(client) -> None:
    payload = agent_env_payload(schema="agent-config/2.0")
    response = (
        await client.post_batch(
            unit("agent.env", f"agentenv:{sha(payload)}", payload, session_id=SESSION)
        )
    ).json()
    assert response["rejected"][0]["code"] == "schema_invalid"


async def test_agent_env_oversize_is_rejected_not_truncated(custom_client) -> None:
    client = await custom_client(agent_env_max_bytes=128)
    payload = agent_env_payload()
    response = (
        await client.post_batch(
            unit("agent.env", f"agentenv:{sha(payload)}", payload, session_id=SESSION)
        )
    ).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert "limit" in response["rejected"][0]["message"]
    assert not await client.object_store.exists("dev", sha(payload))


async def test_session_meta_records_agent_config_anchor(client) -> None:
    content = snapshot_bytes(512 * 1024)
    content_hash, _chunks = await send_snapshot(client, content)

    agent_payload = agent_env_payload()
    agent_sha = sha(agent_payload)
    await client.post_batch(
        unit("agent.env", f"agentenv:{agent_sha}", agent_payload, session_id=SESSION)
    )
    response = (
        await client.post_batch(
            unit(
                "session.meta",
                f"meta:{content_hash}",
                session_meta_payload(content_hash, agent_config_sha=agent_sha),
                session_id=SESSION,
            )
        )
    ).json()
    assert response["accepted"], response

    session = await client.repository.get_session("dev", SESSION)
    assert session is not None
    assert session.agent_config_sha256 == agent_sha
    # 正文不入 PG：仓储里只有内容寻址锚点，没有 agent 配置正文。
    assert not hasattr(session, "agent_config_body")


async def test_session_meta_is_deferred_when_snapshot_missing(client) -> None:
    agent_payload = agent_env_payload()
    agent_sha = sha(agent_payload)
    await client.post_batch(
        unit("agent.env", f"agentenv:{agent_sha}", agent_payload, session_id=SESSION)
    )
    content_hash = "b" * 64
    response = (
        await client.post_batch(
            unit(
                "session.meta",
                f"meta:{content_hash}",
                session_meta_payload(content_hash, agent_config_sha=agent_sha),
                session_id=SESSION,
            )
        )
    ).json()
    assert len(response["deferred"]) == 1, response
    assert await client.repository.get_session("dev", SESSION) is None


async def test_session_meta_invalid_workspace_kind_is_rejected(client) -> None:
    content = snapshot_bytes(1024)
    content_hash, _ = await send_snapshot(client, content)
    response = (
        await client.post_batch(
            unit(
                "session.meta",
                f"meta:{content_hash}",
                session_meta_payload(content_hash, workspace_kind="submodule"),
                session_id=SESSION,
            )
        )
    ).json()
    assert response["rejected"][0]["code"] == "schema_invalid"


async def test_stale_session_meta_does_not_roll_back_anchor(client) -> None:
    content = snapshot_bytes(1024)
    content_hash, _ = await send_snapshot(client, content)
    newer = session_meta_payload(content_hash, updated_at="2026-11-06T12:00:00Z", title="new")
    older = session_meta_payload(content_hash, updated_at="2026-11-06T08:00:00Z", title="old")

    first = (
        await client.post_batch(
            unit("session.meta", f"meta:{content_hash}", newer, session_id=SESSION)
        )
    ).json()
    assert first["accepted"], first
    second = (
        await client.post_batch(
            unit("session.meta", f"meta:{content_hash}", older, session_id=SESSION)
        )
    ).json()
    # 落后值不回退：单元本身被接受（收据 committed），但锚点保持最新那份。
    assert second["accepted"], second

    session = await client.repository.get_session("dev", SESSION)
    assert session is not None
    assert session.title == "new"
