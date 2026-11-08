"""PostgresRepository 真库冒烟测试（环境变量门控，语义对照 MemoryRepository）。

用法（一次性本地集群，别装系统服务）：

    $env:ICODE_DS_TEST_POSTGRES_DSN = 'postgresql://postgres@127.0.0.1:55433/postgres'
    uv run pytest tests/test_postgres_integration.py -q

未设置 ICODE_DS_TEST_POSTGRES_DSN 时整个模块跳过。

隔离方式：每个用例独占一个随机 schema——先建 schema，再把 `search_path=...`
追加到 DSN（asyncpg 会把未识别的 query 参数放进 startup 包，连接级生效），
`PostgresRepository.start()` 的 auto_migrate 随即把 schema.sql 幂等应用到该
schema；用完 `drop schema ... cascade`。

注意：`PostgresRepository` 用 ContextVar 复用事务连接，事务内不得并发发起
池化查询——本模块所有用例都是顺序 await（嵌套事务复用最外层连接）。

xfail（strict）用例钉住的是已确认的 postgres.py 真 bug（P0-4 集成发现，
复现/根因/建议修法见交付报告）；修复后应 XPASS 并移除标记。
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest
import pytest_asyncio

from icode_data_server.storage.models import (
    OBJECT_MISSING,
    OBJECT_VERIFIED,
    RECEIPT_COMMITTED,
    RECEIPT_REJECTED,
    BatchRecord,
    EventRecord,
    ObjectRecord,
    ReceiptRecord,
    RepositoryRecord,
    SessionRecord,
    TurnRecord,
    WorkspaceStateRecord,
)
from icode_data_server.storage.postgres import PostgresRepository

pytestmark = pytest.mark.skipif(
    not os.environ.get("ICODE_DS_TEST_POSTGRES_DSN"),
    reason="set ICODE_DS_TEST_POSTGRES_DSN to enable real-database tests",
)

_DSN = os.environ.get("ICODE_DS_TEST_POSTGRES_DSN", "")

TENANT = "tenant-1"
OTHER_TENANT = "tenant-2"
SESSION = "session-1"
T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


@dataclass
class PostgresHarness:
    repository: PostgresRepository
    dsn: str
    schema: str


@pytest_asyncio.fixture
async def harness() -> AsyncIterator[PostgresHarness]:
    schema = f"icode_test_{uuid.uuid4().hex[:12]}"
    admin = await asyncpg.connect(_DSN)
    try:
        await admin.execute(f'create schema "{schema}"')
    finally:
        await admin.close()
    separator = "&" if "?" in _DSN else "?"
    dsn = f"{_DSN}{separator}search_path={schema}"
    repository = PostgresRepository(dsn, min_size=1, max_size=4)
    try:
        await repository.start()  # auto_migrate 在独立 schema 里应用 schema.sql
        yield PostgresHarness(repository=repository, dsn=_DSN, schema=schema)
    finally:
        await repository.close()
        cleanup = await asyncpg.connect(_DSN)
        try:
            await cleanup.execute(f'drop schema if exists "{schema}" cascade')
        finally:
            await cleanup.close()


@pytest_asyncio.fixture
async def repo(harness: PostgresHarness) -> AsyncIterator[PostgresRepository]:
    yield harness.repository


# ── 构造器 ──────────────────────────────────────────────────────────────────


def _receipt(unit_id: str = "unit-1", **overrides: Any) -> ReceiptRecord:
    record = ReceiptRecord(tenant=TENANT, unit_id=unit_id, sha256="a" * 64, state=RECEIPT_COMMITTED)
    return replace(record, **overrides) if overrides else record


def _session(**overrides: Any) -> SessionRecord:
    record = SessionRecord(
        tenant=TENANT,
        session_id=SESSION,
        user_id="user-1",
        content_hash="c" * 64,
        observed_at=T0,
        updated_at=T0,
        created_at=T0 - timedelta(days=1),
        title="v1",
        repository_identities=("repo-a",),
        snapshot_sha256="s" * 64,
        snapshot_size=1024,
        snapshot_state="ok",
    )
    return replace(record, **overrides) if overrides else record


def _event(sequence: int, **overrides: Any) -> EventRecord:
    record = EventRecord(
        tenant=TENANT,
        session_id=SESSION,
        generation=1,
        sequence=sequence,
        batch_id="batch-1",
        event_id=f"event-{sequence}",
        event_type="tool.call",
        turn_id="turn-1",
        operation_id=f"op-{sequence}",
        actor_kind="agent",
        actor_role="main",
        occurred_at=T0 + timedelta(seconds=sequence),
        monotonic_ns=sequence * 1000,
        tool_name="read_file",
        tool_kind="file",
        duration_ms=10,
        outcome="ok",
        output_tokens=5,
        input_tokens=7,
    )
    return replace(record, **overrides) if overrides else record


def _workspace(**overrides: Any) -> WorkspaceStateRecord:
    record = WorkspaceStateRecord(
        tenant=TENANT,
        session_id=SESSION,
        repository_identity="repo-a",
        user_id="user-1",
        remote_kind="origin",
        body_sha256="w" * 64,
        body_size_bytes=1234,
        head_sha="f" * 40,
        head_tree_sha="e" * 40,
        branch="main",
        staged_entries=2,
        worktree_entries=1,
        untracked_entries=3,
        session_started_at=T0,
        captured_at=T0,
        capture_duration_ms=42,
        ignored_count=2,
        ignored_entries=("build/", "dist/"),
        body_etag="etag-w",
    )
    return replace(record, **overrides) if overrides else record


def _object(sha256: str, state: str, **overrides: Any) -> ObjectRecord:
    record = ObjectRecord(
        tenant=TENANT, sha256=sha256, size_bytes=100, channel="snapshot", state=state
    )
    return replace(record, **overrides) if overrides else record


# ── schema 应用与隔离 ───────────────────────────────────────────────────────


async def test_auto_migrate_applies_schema_to_isolated_schema(
    harness: PostgresHarness,
) -> None:
    admin = await asyncpg.connect(harness.dsn)
    try:
        rows = await admin.fetch(
            "select table_name from information_schema.tables"
            " where table_schema = $1 and table_type = 'BASE TABLE'",
            harness.schema,
        )
    finally:
        await admin.close()
    tables = {row["table_name"] for row in rows}
    assert {
        "schema_meta",
        "sessions",
        "session_turns",
        "event_batches",
        "trajectory_events",
        "object_registry",
        "ingest_receipts",
        "workspace_states",
        "repository_registry",
        "session_anchors",
    } <= tables


async def test_fresh_schema_is_empty(repo: PostgresRepository) -> None:
    assert await repo.get_receipt(TENANT, "unit-1") is None
    assert await repo.get_object(TENANT, "a" * 64) is None
    assert await repo.get_session(TENANT, SESSION) is None
    assert await repo.max_committed_sequence(TENANT, SESSION) == 0
    assert await repo.list_events(TENANT, SESSION) == []
    assert await repo.list_turns(TENANT, SESSION) == []


# ── 幂等收据 ────────────────────────────────────────────────────────────────


async def test_put_receipt_is_idempotent_and_get_receipt_round_trips(
    repo: PostgresRepository,
) -> None:
    assert await repo.put_receipt(_receipt("unit-1")) is True
    # 第二次（即便内容不同）不覆盖：on conflict do nothing
    assert (
        await repo.put_receipt(
            _receipt("unit-1", sha256="b" * 64, state=RECEIPT_REJECTED, code="E1", message="no")
        )
        is False
    )
    stored = await repo.get_receipt(TENANT, "unit-1")
    assert stored is not None
    assert stored.unit_id == "unit-1"
    assert stored.sha256 == "a" * 64
    assert stored.state == RECEIPT_COMMITTED
    assert stored.channel == "report"
    assert stored.code is None
    assert stored.message is None
    assert stored.created_at is not None  # DDL default now()
    assert await repo.get_receipt(OTHER_TENANT, "unit-1") is None


# ── 对象登记 ────────────────────────────────────────────────────────────────


async def test_put_object_state_progresses_one_way_to_verified(
    repo: PostgresRepository,
) -> None:
    sha_a = "a" * 64
    await repo.put_object(
        _object(sha_a, OBJECT_MISSING, missing_hashes=("m1", "m2"), size_bytes=100)
    )
    stored = await repo.get_object(TENANT, sha_a)
    assert stored is not None
    assert stored.state == OBJECT_MISSING
    assert stored.size_bytes == 100
    assert stored.missing_hashes == ("m1", "m2")
    assert stored.channel == "snapshot"
    assert stored.created_at is not None

    # missing → verified 允许
    await repo.put_object(_object(sha_a, OBJECT_VERIFIED, size_bytes=120))
    stored = await repo.get_object(TENANT, sha_a)
    assert stored is not None
    assert stored.state == OBJECT_VERIFIED
    assert stored.size_bytes == 120
    assert stored.missing_hashes == ()

    # verified → missing 不生效（upsert 的 case 分支只保护 state）
    await repo.put_object(_object(sha_a, OBJECT_MISSING, size_bytes=999, missing_hashes=("m3",)))
    stored = await repo.get_object(TENANT, sha_a)
    assert stored is not None
    assert stored.state == OBJECT_VERIFIED
    # 与 SQL 文本一致：size_bytes / missing_hashes 仍按最新值覆盖
    assert stored.size_bytes == 999
    assert stored.missing_hashes == ("m3",)

    # 全新行的 etag 正常往返
    await repo.put_object(_object("f" * 64, OBJECT_VERIFIED, etag="etag-fresh"))
    fresh = await repo.get_object(TENANT, "f" * 64)
    assert fresh is not None
    assert fresh.etag == "etag-fresh"

    assert await repo.get_object(OTHER_TENANT, sha_a) is None
    assert await repo.get_object(TENANT, "0" * 64) is None


async def test_put_object_upsert_updates_etag(repo: PostgresRepository) -> None:
    sha_a = "a" * 64
    await repo.put_object(_object(sha_a, OBJECT_MISSING))
    await repo.put_object(_object(sha_a, OBJECT_VERIFIED, size_bytes=120, etag="etag-1"))
    stored = await repo.get_object(TENANT, sha_a)
    assert stored is not None
    assert stored.etag == "etag-1"


async def test_state_check_constraints_are_created_per_schema() -> None:
    """补约束的 DO 块必须按表判断：`conname` 跨 schema 不唯一，只按名字判会漏建。

    两个全新 schema 各自 migrate 一次：都被加上 CHECK 约束，且越界 `state` 被拒绝。
    """
    schemas = [f"icode_test_{uuid.uuid4().hex[:10]}" for _ in range(2)]
    separator = "&" if "?" in _DSN else "?"
    admin = await asyncpg.connect(_DSN)
    try:
        for schema in schemas:
            await admin.execute(f'create schema "{schema}"')
        for schema in schemas:
            repository = PostgresRepository(f"{_DSN}{separator}search_path={schema}")
            await repository.start()
            await repository.close()
        rows = await admin.fetch(
            "select n.nspname as schema_name, c.conname as constraint_name"
            " from pg_constraint c"
            " join pg_class t on t.oid = c.conrelid"
            " join pg_namespace n on n.oid = t.relnamespace"
            " where n.nspname = any($1::text[]) and c.contype = 'c'",
            schemas,
        )
        found = {(row["schema_name"], row["constraint_name"]) for row in rows}
        for schema in schemas:
            assert (schema, "object_registry_state_check") in found
            assert (schema, "ingest_receipts_state_check") in found
        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await admin.execute(
                f'insert into "{schemas[0]}".object_registry'
                " (tenant, sha256, size_bytes, channel, state)"
                " values ('tenant-1', 'a', 1, 'session-snapshot', 'bogus')"
            )
    finally:
        for schema in schemas:
            await admin.execute(f'drop schema if exists "{schema}" cascade')
        await admin.close()


async def test_verified_objects_only_returns_verified(repo: PostgresRepository) -> None:
    sha_verified, sha_missing = "a" * 64, "b" * 64
    await repo.put_object(_object(sha_verified, OBJECT_VERIFIED))
    await repo.put_object(_object(sha_missing, OBJECT_MISSING))
    assert await repo.verified_objects(TENANT, [sha_verified, sha_missing, "c" * 64]) == {
        sha_verified
    }
    assert await repo.verified_objects(TENANT, []) == set()
    assert await repo.verified_objects(OTHER_TENANT, [sha_verified]) == set()


# ── 会话 ────────────────────────────────────────────────────────────────────


async def test_agent_config_anchor_survives_without_session_row(
    repo: PostgresRepository,
) -> None:
    """agent.env 先于 session.meta 到达：锚点先落 session_anchors（不依赖 sessions 行）。"""
    anchor_sha = "d" * 64
    captured = T0
    await repo.set_agent_config_anchor(
        TENANT, SESSION, anchor_sha, workspace_kind="non_git", captured_at=captured
    )
    assert await repo.get_agent_config_anchor(TENANT, SESSION) == anchor_sha
    state = await repo.get_agent_config_anchor_state(TENANT, SESSION)
    assert state is not None
    assert state.agent_config_sha256 == anchor_sha
    assert state.workspace_kind == "non_git"
    assert state.agent_config_captured_at == captured
    assert await repo.get_agent_config_anchor_state(TENANT, "unknown") is None

    new_anchor = "e" * 64
    await repo.set_agent_config_anchor(TENANT, SESSION, new_anchor)
    assert await repo.get_agent_config_anchor(TENANT, SESSION) == new_anchor
    # 新 blob 未带信封事实时保留上一份的分类与采集时刻
    state = await repo.get_agent_config_anchor_state(TENANT, SESSION)
    assert state is not None
    assert state.agent_config_sha256 == new_anchor
    assert state.workspace_kind == "non_git"
    assert state.agent_config_captured_at == captured
    assert await repo.get_agent_config_anchor(TENANT, "unknown") is None


async def test_upsert_session_monotonic_progress_and_stale_rollback(
    repo: PostgresRepository,
) -> None:
    base = _session()
    assert await repo.upsert_session(base) is True
    stored = await repo.get_session(TENANT, SESSION)
    assert stored is not None
    assert stored.title == "v1"
    assert stored.observed_at == T0
    assert stored.updated_at == T0
    assert stored.created_at == T0 - timedelta(days=1)
    assert stored.repository_identities == ("repo-a",)
    assert stored.snapshot_sha256 == "s" * 64
    assert stored.snapshot_size == 1024

    # 完全平手（updated_at、observed_at 都不变）不算前进
    assert await repo.upsert_session(replace(base, title="tie")) is False
    # stale 报告不前进：updated_at 落后 → 返回 False，不覆盖
    assert (
        await repo.upsert_session(
            replace(base, title="stale", updated_at=T0 - timedelta(seconds=1))
        )
        is False
    )
    stored = await repo.get_session(TENANT, SESSION)
    assert stored is not None
    assert stored.title == "v1"

    # 平手裁决：updated_at 相同、observed_at 更新 → 前进
    assert (
        await repo.upsert_session(
            replace(base, title="tie-break", observed_at=T0 + timedelta(seconds=5))
        )
        is True
    )

    # 新报告前进；repository_identities 单调累积（≤16）
    assert (
        await repo.upsert_session(
            replace(
                base,
                title="newer",
                updated_at=T0 + timedelta(minutes=1),
                observed_at=T0 + timedelta(minutes=1),
                repository_identities=("repo-b",),
            )
        )
        is True
    )
    stored = await repo.get_session(TENANT, SESSION)
    assert stored is not None
    assert stored.title == "newer"
    assert stored.observed_at == T0 + timedelta(minutes=1)
    assert stored.updated_at == T0 + timedelta(minutes=1)
    assert set(stored.repository_identities) == {"repo-a", "repo-b"}
    # created_at 不被新报告覆盖
    assert stored.created_at == T0 - timedelta(days=1)

    assert await repo.get_session(OTHER_TENANT, SESSION) is None
    assert await repo.get_session(TENANT, "unknown") is None


async def test_agent_config_anchor_round_trip(repo: PostgresRepository) -> None:
    anchor_sha = "d" * 64
    await repo.set_agent_config_anchor(TENANT, SESSION, anchor_sha)
    assert await repo.get_agent_config_anchor(TENANT, SESSION) == anchor_sha

    await repo.upsert_session(_session())
    assert await repo.get_agent_config_anchor(TENANT, SESSION) == anchor_sha

    # 锚点更新时同步刷 sessions 行
    new_anchor = "e" * 64
    await repo.set_agent_config_anchor(TENANT, SESSION, new_anchor)
    assert await repo.get_agent_config_anchor(TENANT, SESSION) == new_anchor
    stored = await repo.get_session(TENANT, SESSION)
    assert stored is not None
    assert stored.agent_config_sha256 == new_anchor


async def test_set_session_body_state(repo: PostgresRepository) -> None:
    await repo.upsert_session(_session())
    await repo.set_session_body_state(TENANT, SESSION, "missing")
    stored = await repo.get_session(TENANT, SESSION)
    assert stored is not None
    assert stored.body_state == "missing"
    # 不存在的会话：无操作、不报错（与 MemoryRepository 一致）
    await repo.set_session_body_state(TENANT, "nope", "missing")


async def test_upsert_session_coalesces_agent_config_anchor(
    repo: PostgresRepository,
) -> None:
    anchor_sha = "d" * 64
    await repo.set_agent_config_anchor(TENANT, SESSION, anchor_sha)
    assert await repo.upsert_session(_session()) is True
    stored = await repo.get_session(TENANT, SESSION)
    assert stored is not None
    assert stored.agent_config_sha256 == anchor_sha


async def test_upsert_session_with_null_updated_at_advances(
    repo: PostgresRepository,
) -> None:
    """SessionRecord.updated_at 允许 None；后续 aware 报告应能正常前进。"""
    assert await repo.upsert_session(_session(updated_at=None)) is True
    assert (
        await repo.upsert_session(
            _session(
                title="v2",
                updated_at=T0 + timedelta(minutes=1),
                observed_at=T0 + timedelta(minutes=1),
            )
        )
        is True
    )
    stored = await repo.get_session(TENANT, SESSION)
    assert stored is not None
    assert stored.title == "v2"


# ── 轮次 ────────────────────────────────────────────────────────────────────


async def test_upsert_turn_and_list_turns(repo: PostgresRepository) -> None:
    first = TurnRecord(
        tenant=TENANT,
        session_id=SESSION,
        generation=1,
        turn_number=1,
        turn_id="turn-1",
        started_at=T0,
        ended_at=T0 + timedelta(seconds=5),
        duration_ms=5000,
        end_reason="end_turn",
    )
    await repo.upsert_turn(first)
    await repo.upsert_turn(
        TurnRecord(tenant=TENANT, session_id=SESSION, generation=1, turn_number=2, turn_id=None)
    )
    # 重复上报：coalesce 语义——None 不覆盖已有值
    await repo.upsert_turn(replace(first, duration_ms=None, end_reason=None))
    turns = await repo.list_turns(TENANT, SESSION)
    assert [(turn.generation, turn.turn_number) for turn in turns] == [(1, 1), (1, 2)]
    merged = turns[0]
    assert merged.turn_id == "turn-1"
    assert merged.duration_ms == 5000
    assert merged.end_reason == "end_turn"
    assert merged.started_at == T0
    assert turns[1].turn_id is None
    assert await repo.list_turns(OTHER_TENANT, SESSION) == []


# ── 事件 ────────────────────────────────────────────────────────────────────


async def test_put_batch_put_events_and_sequences(repo: PostgresRepository) -> None:
    await repo.put_batch(
        BatchRecord(
            tenant=TENANT,
            session_id=SESSION,
            batch_id="batch-1",
            generation=1,
            client_sequence=1,
            first_event_sequence=1,
            last_event_sequence=3,
            event_count=3,
            unparsed_count=0,
            gap_count=0,
            completeness_state="complete",
            payload_sha256="p" * 64,
            payload_size=999,
        )
    )
    await repo.put_events([_event(n) for n in (1, 2, 3)])
    assert await repo.max_committed_sequence(TENANT, SESSION) == 3
    listed = await repo.list_events(TENANT, SESSION)
    assert [event.sequence for event in listed] == [1, 2, 3]
    assert listed[0].event_id == "event-1"
    assert listed[0].event_type == "tool.call"
    assert listed[0].occurred_at == T0 + timedelta(seconds=1)
    assert listed[0].monotonic_ns == 1000
    assert listed[2].output_tokens == 5

    # 重复 put：on conflict (tenant, session_id, generation, sequence) do nothing
    await repo.put_events([replace(_event(2), event_id="mutated", event_type="mutated")])
    listed = await repo.list_events(TENANT, SESSION)
    assert len(listed) == 3
    assert listed[1].event_id == "event-2"
    assert listed[1].event_type == "tool.call"

    assert await repo.max_committed_sequence(OTHER_TENANT, SESSION) == 0
    assert await repo.list_events(OTHER_TENANT, SESSION) == []


# ── 工作区 ──────────────────────────────────────────────────────────────────


async def test_upsert_workspace_state_and_list(repo: PostgresRepository) -> None:
    await repo.upsert_workspace_state(_workspace())
    await repo.upsert_workspace_state(
        _workspace(repository_identity="repo-b", body_sha256="x" * 64)
    )
    listed = await repo.list_workspace_states(TENANT, SESSION)
    assert {state.repository_identity for state in listed} == {"repo-a", "repo-b"}
    got = next(state for state in listed if state.repository_identity == "repo-a")
    assert got.body_sha256 == "w" * 64
    assert got.body_size_bytes == 1234
    assert got.head_sha == "f" * 40
    assert got.branch == "main"
    assert got.detached is False
    assert got.staged_entries == 2
    assert got.captured_at == T0
    assert got.ignored_count == 2
    assert got.ignored_entries == ("build/", "dist/")  # jsonb 往返
    assert got.body_etag == "etag-w"

    # stale 捕获不回退（captured_at 落后 → 不覆盖）
    await repo.upsert_workspace_state(
        _workspace(captured_at=T0 - timedelta(seconds=10), body_sha256="z" * 64)
    )
    got = next(
        state
        for state in await repo.list_workspace_states(TENANT, SESSION)
        if state.repository_identity == "repo-a"
    )
    assert got.body_sha256 == "w" * 64
    # 更新的捕获覆盖
    await repo.upsert_workspace_state(
        _workspace(captured_at=T0 + timedelta(seconds=10), body_sha256="y" * 64)
    )
    got = next(
        state
        for state in await repo.list_workspace_states(TENANT, SESSION)
        if state.repository_identity == "repo-a"
    )
    assert got.body_sha256 == "y" * 64
    assert await repo.list_workspace_states(OTHER_TENANT, SESSION) == []


# ── 仓库登记 ────────────────────────────────────────────────────────────────


async def test_upsert_repository_and_list(repo: PostgresRepository) -> None:
    base = RepositoryRecord(
        tenant=TENANT,
        local_identity="repo-a",
        repository_key="github.com/o/r",
        provider="github",
        connection_id="conn-1",
        host="github.com",
        api_path="/o/r",
        resolution="remote",
        user_id="user-1",
        observed_at=T0,
    )
    await repo.upsert_repository(base)
    await repo.upsert_repository(
        replace(base, repository_key="github.com/o/r2", observed_at=T0 + timedelta(minutes=1))
    )
    listed = await repo.list_repositories(TENANT)
    assert len(listed) == 1
    got = listed[0]
    assert got.local_identity == "repo-a"
    assert got.repository_key == "github.com/o/r2"  # 最新值覆盖
    assert got.connection_id == "conn-1"
    assert got.observed_at == T0 + timedelta(minutes=1)
    assert got.first_seen_at is not None  # 首见时间保留
    assert got.last_seen_at is not None
    assert await repo.list_repositories(OTHER_TENANT) == []


# ── 事务 ────────────────────────────────────────────────────────────────────


async def test_transaction_commits_on_success(repo: PostgresRepository) -> None:
    async with repo.transaction():
        assert await repo.put_receipt(_receipt("unit-1")) is True
        await repo.put_events([_event(1), _event(2)])
        # 事务内读写走同一连接（ContextVar 复用）
        assert await repo.get_receipt(TENANT, "unit-1") is not None
    assert await repo.get_receipt(TENANT, "unit-1") is not None
    assert await repo.max_committed_sequence(TENANT, SESSION) == 2


async def test_transaction_rolls_back_on_error(repo: PostgresRepository) -> None:
    with pytest.raises(RuntimeError):
        async with repo.transaction():
            assert await repo.put_receipt(_receipt("unit-1")) is True
            await repo.put_object(_object("a" * 64, OBJECT_VERIFIED))
            # 嵌套事务复用最外层连接：一并回滚
            async with repo.transaction():
                await repo.put_events([_event(1), _event(2)])
                assert await repo.max_committed_sequence(TENANT, SESSION) == 2
            raise RuntimeError("boom")
    assert await repo.get_receipt(TENANT, "unit-1") is None
    assert await repo.get_object(TENANT, "a" * 64) is None
    assert await repo.max_committed_sequence(TENANT, SESSION) == 0
    # 回滚后仓储仍可用
    assert await repo.put_receipt(_receipt("unit-1")) is True
