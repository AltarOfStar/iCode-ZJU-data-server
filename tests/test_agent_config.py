"""`agent.env` 存取与 `agent-config/1.0` 校验（agent-config-sync §1，权威规范 §4.6）。

三列（`agent_config_sha256` / `workspace_kind` / `agent_config_captured_at`）的唯一
写入者是 `agent.env`（含信封 `workspaceKind` / `capturedAt`）；`session.meta` 不携带
`environment`，携带即按未知字段显式拒绝。
"""

from __future__ import annotations

import json
from pathlib import Path

from .helpers import (
    SESSION,
    agent_env_frame,
    agent_env_payload,
    send_snapshot,
    session_meta_payload,
    sha,
    snapshot_bytes,
    unit,
    unit_id,
)


async def test_agent_env_is_content_addressed_and_anchored(client) -> None:
    payload = agent_env_payload()
    digest = sha(payload)
    response = (await client.post_batch(agent_env_frame(payload))).json()
    assert response["accepted"], response

    assert await client.object_store.get("dev", digest) == payload
    record = await client.repository.get_object("dev", digest)
    assert record is not None and record.state == "verified"
    assert record.channel == "agent-config"
    assert await client.repository.get_agent_config_anchor("dev", SESSION) == digest


async def test_agent_env_envelope_fields_are_required(client) -> None:
    def _payload(tag: str) -> bytes:
        return agent_env_payload(redacted=[tag])

    # 信封字段不参与 unit_id 派生：四个变体用不同载荷，各自独立成单元。
    response = (
        await client.post_batch(
            unit("agent.env", f"agentenv:{sha(_payload('a'))}", _payload("a"), session_id=SESSION)
        )
    ).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert "workspaceKind" in response["rejected"][0]["message"]

    response = (
        await client.post_batch(
            unit(
                "agent.env",
                f"agentenv:{sha(_payload('b'))}",
                _payload("b"),
                session_id=SESSION,
                extra_header={"workspaceKind": "git"},
            )
        )
    ).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert "capturedAt" in response["rejected"][0]["message"]

    response = (
        await client.post_batch(
            agent_env_frame(
                _payload("c"), workspace_kind="submodule", captured_at="2026-11-06T09:59:58Z"
            )
        )
    ).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert "workspaceKind" in response["rejected"][0]["message"]

    response = (
        await client.post_batch(
            agent_env_frame(_payload("d"), workspace_kind="git", captured_at="not-a-timestamp")
        )
    ).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert "capturedAt" in response["rejected"][0]["message"]
    assert await client.repository.get_object("dev", sha(_payload("d"))) is None


async def test_agent_env_payload_requires_sections_and_truncated(client) -> None:
    missing_sections = json.loads(agent_env_payload())
    missing_sections.pop("sections")
    response = (
        await client.post_batch(
            agent_env_frame(json.dumps(missing_sections, separators=(",", ":")).encode())
        )
    ).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert "sections" in response["rejected"][0]["message"]

    bad_state = json.loads(agent_env_payload())
    bad_state["sections"]["mcp"] = "unknown"
    response = (await client.post_batch(agent_env_frame(json.dumps(bad_state).encode()))).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert "sections.mcp" in response["rejected"][0]["message"]

    extra_key = json.loads(agent_env_payload())
    extra_key["sections"]["tools"] = "captured"
    response = (await client.post_batch(agent_env_frame(json.dumps(extra_key).encode()))).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert "sections" in response["rejected"][0]["message"]

    document = json.loads(agent_env_payload())
    document.pop("truncated")
    response = (
        await client.post_batch(
            agent_env_frame(json.dumps(document, separators=(",", ":")).encode())
        )
    ).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert "truncated" in response["rejected"][0]["message"]

    # 合法载荷：某来源 absent、truncated=true（消费方必须容忍其它生产方降级）
    payload = agent_env_payload(absent=("mcp",), truncated=True)
    response = (await client.post_batch(agent_env_frame(payload))).json()
    assert response["accepted"], response


async def test_agent_env_missing_redacted_is_rejected(client) -> None:
    document = json.loads(agent_env_payload())
    document.pop("redacted")
    payload = json.dumps(document, separators=(",", ":")).encode()
    response = (await client.post_batch(agent_env_frame(payload))).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert "redacted" in response["rejected"][0]["message"]
    assert not await client.object_store.exists("dev", sha(payload))


async def test_agent_env_wrong_schema_is_rejected(client) -> None:
    payload = agent_env_payload(schema="agent-config/2.0")
    response = (await client.post_batch(agent_env_frame(payload))).json()
    assert response["rejected"][0]["code"] == "schema_invalid"


async def test_agent_env_oversize_is_rejected_not_truncated(custom_client) -> None:
    client = await custom_client(agent_env_max_bytes=128)
    payload = agent_env_payload()
    response = (await client.post_batch(agent_env_frame(payload))).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert "limit" in response["rejected"][0]["message"]
    assert not await client.object_store.exists("dev", sha(payload))


async def test_agent_env_before_session_meta_fills_three_columns(client) -> None:
    """早到锚点：agent.env 先到，session.meta 建行时从锚点 coalesce 三列。"""
    content = snapshot_bytes(512 * 1024)
    content_hash, _chunks = await send_snapshot(client, content)

    agent_payload = agent_env_payload()
    agent_sha = sha(agent_payload)
    await client.post_batch(
        agent_env_frame(agent_payload, workspace_kind="non_git", captured_at="2026-11-06T09:59:58Z")
    )
    response = (
        await client.post_batch(
            unit(
                "session.meta",
                f"meta:{content_hash}",
                session_meta_payload(content_hash),
                session_id=SESSION,
            )
        )
    ).json()
    assert response["accepted"], response

    session = await client.repository.get_session("dev", SESSION)
    assert session is not None
    assert session.agent_config_sha256 == agent_sha
    assert session.workspace_kind == "non_git"
    assert session.agent_config_captured_at is not None
    assert session.agent_config_captured_at.isoformat().startswith("2026-11-06T09:59:58")
    # 正文不入 PG：仓储里只有内容寻址锚点，没有 agent 配置正文。
    assert not hasattr(session, "agent_config_body")


async def test_session_meta_without_agent_env_is_not_deferred(client) -> None:
    """session.meta 不再因 agent-config 未转正而 deferred（agent-config-sync §2-1）。"""
    content = snapshot_bytes(512 * 1024)
    content_hash, _chunks = await send_snapshot(client, content)

    response = (
        await client.post_batch(
            unit(
                "session.meta",
                f"meta:{content_hash}",
                session_meta_payload(content_hash),
                session_id=SESSION,
            )
        )
    ).json()
    assert response["accepted"], response
    session = await client.repository.get_session("dev", SESSION)
    assert session is not None
    assert session.agent_config_sha256 is None
    assert session.workspace_kind is None
    assert session.agent_config_captured_at is None


async def test_agent_env_after_session_meta_updates_three_columns(client) -> None:
    """晚到补采：行已在，agent.env 直接推进三列（会话前采集 vs 后补由此可区分）。"""
    content = snapshot_bytes(512 * 1024)
    content_hash, _chunks = await send_snapshot(client, content)
    await client.post_batch(
        unit(
            "session.meta",
            f"meta:{content_hash}",
            session_meta_payload(content_hash),
            session_id=SESSION,
        )
    )

    agent_payload = agent_env_payload()
    agent_sha = sha(agent_payload)
    response = (
        await client.post_batch(
            agent_env_frame(
                agent_payload, workspace_kind="no_repo", captured_at="2026-11-06T11:00:00Z"
            )
        )
    ).json()
    assert response["accepted"], response

    session = await client.repository.get_session("dev", SESSION)
    assert session is not None
    assert session.agent_config_sha256 == agent_sha
    assert session.workspace_kind == "no_repo"
    assert session.agent_config_captured_at is not None
    assert session.agent_config_captured_at.isoformat().startswith("2026-11-06T11:00:00")


async def test_agent_env_overwrite_supersedes_previous_object(client) -> None:
    """覆写汰换：同会话换新配置 → 旧引用对象置 superseded，OBS 字节保留。"""
    first_payload = agent_env_payload()
    first_sha = sha(first_payload)
    await client.post_batch(agent_env_frame(first_payload))

    second_payload = agent_env_payload(redacted=["api_key"])
    second_sha = sha(second_payload)
    assert second_sha != first_sha
    response = (await client.post_batch(agent_env_frame(second_payload))).json()
    assert response["accepted"], response

    old = await client.repository.get_object("dev", first_sha)
    assert old is not None and old.state == "superseded"
    # OBS 字节永不删除
    assert await client.object_store.get("dev", first_sha) == first_payload
    new = await client.repository.get_object("dev", second_sha)
    assert new is not None and new.state == "verified"
    assert await client.repository.get_agent_config_anchor("dev", SESSION) == second_sha


async def test_session_meta_is_deferred_when_snapshot_missing(client) -> None:
    agent_payload = agent_env_payload()
    await client.post_batch(agent_env_frame(agent_payload))
    content_hash = "b" * 64
    response = (
        await client.post_batch(
            unit(
                "session.meta",
                f"meta:{content_hash}",
                session_meta_payload(content_hash),
                session_id=SESSION,
            )
        )
    ).json()
    assert len(response["deferred"]) == 1, response
    assert await client.repository.get_session("dev", SESSION) is None


async def test_session_meta_with_environment_is_rejected(client) -> None:
    """`session/1.0` 不携带环境引用：environment 按未知字段显式拒绝，不做兼容读取。"""
    content = snapshot_bytes(1024)
    content_hash, _ = await send_snapshot(client, content)
    payload = json.loads(session_meta_payload(content_hash))
    payload["environment"] = {"workspaceKind": "git", "agentConfigSha256": "a" * 64}
    response = (
        await client.post_batch(
            unit(
                "session.meta",
                f"meta:{content_hash}",
                json.dumps(payload).encode(),
                session_id=SESSION,
            )
        )
    ).json()
    assert response["rejected"][0]["code"] == "schema_invalid"
    assert "environment" in response["rejected"][0]["message"]
    assert await client.repository.get_session("dev", SESSION) is None


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
    agent_payload = agent_env_payload()
    await client.post_batch(agent_env_frame(agent_payload))
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
    # 三列只由 agent.env 写：落后的 session.meta 不清除
    assert session.agent_config_sha256 == sha(agent_payload)


async def test_agent_env_redelivery_advances_the_envelope(client) -> None:
    """同内容、新分类：重报仍是 duplicate，但三列收敛到较新一次的信封事实。

    分类不参与内容寻址（unit_id 不含它），客户端靠重发同一单元把新分类交给服务端
    覆写（client-design §4.4）——服务端不能把它当普通重复直接丢弃。
    """
    content = snapshot_bytes(512 * 1024)
    content_hash, _ = await send_snapshot(client, content)
    await client.post_batch(
        unit(
            "session.meta",
            f"meta:{content_hash}",
            session_meta_payload(content_hash),
            session_id=SESSION,
        )
    )

    payload = agent_env_payload()
    digest = sha(payload)
    first = (
        await client.post_batch(
            agent_env_frame(payload, workspace_kind="git", captured_at="2026-11-06T09:59:58Z")
        )
    ).json()
    assert first["accepted"], first

    response = (
        await client.post_batch(
            agent_env_frame(payload, workspace_kind="no_repo", captured_at="2026-11-06T10:30:00Z")
        )
    ).json()
    assert response["accepted"] == []
    assert response["duplicates"] == [unit_id("agent.env", f"agentenv:{digest}", payload)]

    session = await client.repository.get_session("dev", SESSION)
    assert session is not None
    assert session.agent_config_sha256 == digest
    assert session.workspace_kind == "no_repo"
    assert session.agent_config_captured_at is not None
    assert session.agent_config_captured_at.isoformat().startswith("2026-11-06T10:30:00")
    # 锚点与对象都不变：没有新 blob，也没有覆写汰换。
    assert await client.repository.get_agent_config_anchor("dev", SESSION) == digest
    record = await client.repository.get_object("dev", digest)
    assert record is not None and record.state == "verified"


async def test_agent_env_redelivery_of_a_superseded_blob_changes_nothing(client) -> None:
    """旧 blob 的重放不得回退锚点，也不得汰换当前对象。"""
    first = agent_env_payload()
    second = agent_env_payload(redacted=["api_key"])
    await client.post_batch(
        agent_env_frame(first, workspace_kind="git", captured_at="2026-11-06T09:00:00Z")
    )
    await client.post_batch(
        agent_env_frame(second, workspace_kind="git", captured_at="2026-11-06T10:00:00Z")
    )

    response = (
        await client.post_batch(
            agent_env_frame(first, workspace_kind="no_repo", captured_at="2026-11-06T11:00:00Z")
        )
    ).json()
    assert response["accepted"] == []
    assert response["duplicates"] == [unit_id("agent.env", f"agentenv:{sha(first)}", first)]

    assert await client.repository.get_agent_config_anchor("dev", SESSION) == sha(second)
    state = await client.repository.get_agent_config_anchor_state("dev", SESSION)
    assert state is not None
    assert state.workspace_kind == "git"
    assert state.agent_config_captured_at is not None
    assert state.agent_config_captured_at.isoformat().startswith("2026-11-06T10:00:00")
    old = await client.repository.get_object("dev", sha(first))
    assert old is not None and old.state == "superseded"
    new = await client.repository.get_object("dev", sha(second))
    assert new is not None and new.state == "verified"


async def test_agent_env_redelivery_does_not_move_captured_at_backwards(client) -> None:
    """乱序重放的旧信封不覆盖已落库的较新事实。"""
    payload = agent_env_payload()
    await client.post_batch(
        agent_env_frame(payload, workspace_kind="git", captured_at="2026-11-06T10:00:00Z")
    )

    response = (
        await client.post_batch(
            agent_env_frame(payload, workspace_kind="no_repo", captured_at="2026-11-06T09:00:00Z")
        )
    ).json()
    assert response["duplicates"] == [unit_id("agent.env", f"agentenv:{sha(payload)}", payload)]

    state = await client.repository.get_agent_config_anchor_state("dev", SESSION)
    assert state is not None
    assert state.workspace_kind == "git"
    assert state.agent_config_captured_at is not None
    assert state.agent_config_captured_at.isoformat().startswith("2026-11-06T10:00:00")


async def test_the_icode_client_payload_is_accepted(client) -> None:
    """跨仓黄金样本：iCode 0.34.0 真实配置根产出的 `agent-config/1.0` 载荷。

    样本由 `service/reporting/agent_config.py` 对一份含 profile/model/技能/MCP
    (stdio+http)/hooks 的布局生成；载荷形状变了就重新产出这一份（别手改）。

    2026-11-08 的格式对齐只改了一个字段：该布局没有显式 hook `settings`，客户端
    因此**不再输出 `settingsDigest`**（旧样本里是 `sha256({})`），已按生产者口径重出一份；
    其余字段（含 `mcp.servers[]` 的 `transport`/`connectionDigest`）逐字未动。下次形状
    再变时仍须从真实配置根重产，不要手改。
    """
    payload = (
        Path(__file__).parent / "fixtures" / "agent_env_icode_agent_config_1_0.json"
    ).read_bytes()
    document = json.loads(payload)
    assert document["schema"] == "agent-config/1.0"
    assert document["truncated"] is False
    assert document["platform"] == "windows-amd64"
    assert sorted(document["sections"]) == ["hooks", "mcp", "model", "profile", "skills"]
    assert set(document["sections"].values()) == {"captured"}
    assert [server["transport"] for server in document["mcp"]["servers"]] == ["stdio", "http"]
    assert document["hooks"]["count"] == 2
    assert document["model"]["params"]["max_output_tokens"] == 8192
    assert b"<redacted>" in payload and b"sk-" not in payload

    response = (await client.post_batch(agent_env_frame(payload))).json()
    assert response["accepted"], response
    assert await client.object_store.get("dev", sha(payload)) == payload
    assert await client.repository.get_agent_config_anchor("dev", SESSION) == sha(payload)
