"""PostgreSQL 仓储实现（asyncpg）。

列与 teams schema v10 在范围内表一致；DDL 见 `schema.sql`（部署期执行，或开发期由
`ICODE_DS_POSTGRES_AUTO_MIGRATE=1` 在启动时幂等应用）。
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path

import asyncpg

from .models import (
    BatchRecord,
    EventRecord,
    ObjectRecord,
    ReceiptRecord,
    RepositoryRecord,
    SessionAnchor,
    SessionRecord,
    TurnRecord,
    WorkspaceStateRecord,
)

_SCHEMA_PATH = Path(__file__).with_name("schema.sql")

_CONNECTION: ContextVar[asyncpg.Connection | None] = ContextVar(
    "icode_data_server_connection", default=None
)


class PostgresRepository:
    def __init__(
        self,
        dsn: str,
        *,
        auto_migrate: bool = True,
        min_size: int = 2,
        max_size: int = 20,
        pool_timeout_s: float = 5.0,
    ):
        self._dsn = dsn
        self._auto_migrate = auto_migrate
        self._min_size = min_size
        self._max_size = max_size
        self._pool_timeout_s = pool_timeout_s
        self._pool: asyncpg.Pool | None = None

    async def start(self) -> None:
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._min_size,
            max_size=self._max_size,
            timeout=self._pool_timeout_s,
        )
        if self._auto_migrate:
            await self.migrate()

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def migrate(self) -> None:
        ddl = _SCHEMA_PATH.read_text(encoding="utf-8")
        assert self._pool is not None
        async with self._pool.acquire() as connection:
            await connection.execute(ddl)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """单事务；连接放在 ContextVar 里，并发任务互不串线（嵌套事务复用最外层）。"""
        assert self._pool is not None, "repository is not started"
        if _CONNECTION.get() is not None:
            yield
            return
        async with self._pool.acquire() as connection, connection.transaction():
            token = _CONNECTION.set(connection)
            try:
                yield
            finally:
                _CONNECTION.reset(token)

    # 幂等收据

    async def get_receipt(self, tenant: str, unit_id: str) -> ReceiptRecord | None:
        row = await self._fetchrow(
            "select tenant, idempotency_key, sha256, state, channel, code, message, created_at"
            " from ingest_receipts where tenant = $1 and channel = 'report'"
            " and idempotency_key = $2",
            tenant,
            unit_id,
        )
        if row is None:
            return None
        return ReceiptRecord(
            tenant=row["tenant"],
            unit_id=row["idempotency_key"],
            sha256=row["sha256"],
            state=row["state"],
            channel=row["channel"],
            code=row["code"],
            message=row["message"],
            created_at=row["created_at"],
        )

    async def put_receipt(self, record: ReceiptRecord) -> bool:
        result = await self._execute(
            "insert into ingest_receipts"
            " (tenant, channel, idempotency_key, sha256, state, code, message)"
            " values ($1, $2, $3, $4, $5, $6, $7)"
            " on conflict (tenant, channel, idempotency_key) do nothing",
            record.tenant,
            record.channel,
            record.unit_id,
            record.sha256,
            record.state,
            record.code,
            record.message,
        )
        return result.endswith("1")

    # 对象登记

    async def put_object(self, record: ObjectRecord) -> None:
        await self._execute(
            "insert into object_registry"
            " (tenant, sha256, size_bytes, etag, channel, state, verified_at, missing_hashes)"
            " values ($1, $2, $3, $4, $5, $6,"
            " case when $6 = 'verified' then now() else null end, $7)"
            " on conflict (tenant, sha256) do update set"
            "   state = case when object_registry.state = 'verified' then 'verified'"
            " else excluded.state end,"
            "   size_bytes = excluded.size_bytes,"
            "   etag = excluded.etag,"
            "   missing_hashes = excluded.missing_hashes,"
            "   verified_at = coalesce(object_registry.verified_at, excluded.verified_at)",
            record.tenant,
            record.sha256,
            record.size_bytes,
            record.etag,
            record.channel,
            record.state,
            list(record.missing_hashes),
        )

    async def get_object(self, tenant: str, sha256: str) -> ObjectRecord | None:
        row = await self._fetchrow(
            "select tenant, sha256, size_bytes, channel, state, etag, missing_hashes, created_at"
            " from object_registry where tenant = $1 and sha256 = $2",
            tenant,
            sha256,
        )
        return None if row is None else _object_from_row(row)

    async def verified_objects(self, tenant: str, shas: Sequence[str]) -> set[str]:
        if not shas:
            return set()
        rows = await self._fetch(
            "select sha256 from object_registry"
            " where tenant = $1 and state = 'verified' and sha256 = any($2::text[])",
            tenant,
            list(shas),
        )
        return {row["sha256"] for row in rows}

    async def mark_object_superseded(self, tenant: str, sha256: str) -> None:
        """覆写汰换（agent-config-sync §1）：旧引用对象置 superseded，OBS 字节保留。"""
        await self._execute(
            "update object_registry set state = 'superseded'"
            " where tenant = $1 and sha256 = $2 and state <> 'superseded'",
            tenant,
            sha256,
        )

    # 会话

    _SESSION_COLUMNS = (
        "tenant, external_session_id, user_id, content_hash, observed_at, updated_at, created_at,"
        " title, agent_profile, model_provider, model_id, producer_version,"
        " parent_external_session_id, primary_cwd_digest, turn_count, message_count, total_tokens,"
        " repository_identities, snapshot_sha256, snapshot_size, snapshot_state, body_state,"
        " agent_config_sha256, workspace_kind, agent_config_captured_at"
    )

    async def get_session(self, tenant: str, session_id: str) -> SessionRecord | None:
        row = await self._fetchrow(
            f"select {self._SESSION_COLUMNS} from sessions"
            " where tenant = $1 and external_session_id = $2",
            tenant,
            session_id,
        )
        if row is None:
            return None
        record = _session_from_row(row)
        return record

    async def upsert_session(self, record: SessionRecord) -> bool:
        """按 `updated_at` 最新、`observed_at` 平手裁决推进锚点；落后值回退不覆盖。"""
        anchor = await self._fetchrow(
            "select updated_at, observed_at from sessions"
            " where tenant = $1 and external_session_id = $2",
            record.tenant,
            record.session_id,
        )
        if anchor is not None:
            previous_updated = anchor["updated_at"]
            previous_observed = anchor["observed_at"]
            if not _is_newer(
                record.updated_at, record.observed_at, previous_updated, previous_observed
            ):
                return False
        # 环境三列只由 agent.env 写（agent-config-sync §1）：session.meta 侧传 NULL，
        # 首插从 session_anchors coalesce，更新分支不写也不清除。
        await self._execute(
            "insert into sessions ("
            + self._SESSION_COLUMNS
            + ") values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,"
            "$20,$21,$22,"
            " coalesce($23, (select agent_config_sha256 from session_anchors"
            "   where tenant = $1 and session_id = $2)),"
            " coalesce($24, (select workspace_kind from session_anchors"
            "   where tenant = $1 and session_id = $2)),"
            " coalesce($25, (select agent_config_captured_at from session_anchors"
            "   where tenant = $1 and session_id = $2)))"
            " on conflict (tenant, external_session_id) do update set"
            "   user_id = excluded.user_id,"
            "   content_hash = excluded.content_hash,"
            "   observed_at = excluded.observed_at,"
            "   updated_at = excluded.updated_at,"
            "   created_at = coalesce(sessions.created_at, excluded.created_at),"
            "   title = excluded.title,"
            "   agent_profile = excluded.agent_profile,"
            "   model_provider = excluded.model_provider,"
            "   model_id = excluded.model_id,"
            "   producer_version = excluded.producer_version,"
            "   parent_external_session_id = excluded.parent_external_session_id,"
            "   primary_cwd_digest = excluded.primary_cwd_digest,"
            "   turn_count = excluded.turn_count,"
            "   message_count = excluded.message_count,"
            "   total_tokens = excluded.total_tokens,"
            "   repository_identities = ("
            "     (select array(select distinct unnest("
            "       sessions.repository_identities || excluded.repository_identities)))[1:16]"
            "   ),"
            "   snapshot_sha256 = excluded.snapshot_sha256,"
            "   snapshot_size = excluded.snapshot_size,"
            "   snapshot_state = excluded.snapshot_state,"
            "   agent_config_sha256 = coalesce(excluded.agent_config_sha256,"
            " sessions.agent_config_sha256),"
            "   workspace_kind = coalesce(excluded.workspace_kind, sessions.workspace_kind),"
            "   agent_config_captured_at = coalesce(excluded.agent_config_captured_at,"
            " sessions.agent_config_captured_at)",
            record.tenant,
            record.session_id,
            record.user_id,
            record.content_hash,
            record.observed_at,
            record.updated_at,
            record.created_at,
            record.title,
            record.agent_profile,
            record.model_provider,
            record.model_id,
            record.producer_version,
            record.parent_session_id,
            record.primary_cwd_digest,
            record.turn_count,
            record.message_count,
            record.total_tokens,
            list(record.repository_identities),
            record.snapshot_sha256,
            record.snapshot_size,
            record.snapshot_state,
            record.body_state,
            record.agent_config_sha256,
            record.workspace_kind,
            record.agent_config_captured_at,
        )
        return True

    async def set_agent_config_anchor(
        self,
        tenant: str,
        session_id: str,
        sha256: str,
        *,
        workspace_kind: str | None = None,
        captured_at: datetime | None = None,
    ) -> None:
        await self._execute(
            "insert into session_anchors"
            " (tenant, session_id, agent_config_sha256, workspace_kind, agent_config_captured_at)"
            " values ($1, $2, $3, $4, $5)"
            " on conflict (tenant, session_id) do update set"
            "   agent_config_sha256 = excluded.agent_config_sha256,"
            "   workspace_kind = coalesce(excluded.workspace_kind, session_anchors.workspace_kind),"
            "   agent_config_captured_at = coalesce(excluded.agent_config_captured_at,"
            "     session_anchors.agent_config_captured_at),"
            "   updated_at = now()",
            tenant,
            session_id,
            sha256,
            workspace_kind,
            captured_at,
        )
        await self._execute(
            "update sessions set"
            " agent_config_sha256 = $3,"
            " workspace_kind = coalesce($4, sessions.workspace_kind),"
            " agent_config_captured_at = coalesce($5, sessions.agent_config_captured_at)"
            " where tenant = $1 and external_session_id = $2",
            tenant,
            session_id,
            sha256,
            workspace_kind,
            captured_at,
        )

    async def get_agent_config_anchor(self, tenant: str, session_id: str) -> str | None:
        row = await self._fetchrow(
            "select agent_config_sha256 from session_anchors where tenant = $1 and session_id = $2",
            tenant,
            session_id,
        )
        if row is not None and row["agent_config_sha256"]:
            return row["agent_config_sha256"]
        row = await self._fetchrow(
            "select agent_config_sha256 from sessions"
            " where tenant = $1 and external_session_id = $2",
            tenant,
            session_id,
        )
        return None if row is None else row["agent_config_sha256"]

    async def get_agent_config_anchor_state(
        self, tenant: str, session_id: str
    ) -> SessionAnchor | None:
        """三列现状：会话行优先（`session.meta` 落地后以行为准），否则用锚点。"""
        row = await self._fetchrow(
            "select agent_config_sha256, workspace_kind, agent_config_captured_at"
            " from sessions where tenant = $1 and external_session_id = $2",
            tenant,
            session_id,
        )
        if row is None or not row["agent_config_sha256"]:
            row = await self._fetchrow(
                "select agent_config_sha256, workspace_kind, agent_config_captured_at"
                " from session_anchors where tenant = $1 and session_id = $2",
                tenant,
                session_id,
            )
        if row is None or not row["agent_config_sha256"]:
            return None
        return SessionAnchor(
            tenant=tenant,
            session_id=session_id,
            agent_config_sha256=row["agent_config_sha256"],
            workspace_kind=row["workspace_kind"],
            agent_config_captured_at=row["agent_config_captured_at"],
        )

    async def set_session_body_state(self, tenant: str, session_id: str, state: str) -> None:
        await self._execute(
            "update sessions set body_state = $3 where tenant = $1 and external_session_id = $2",
            tenant,
            session_id,
            state,
        )

    # 轮次

    async def upsert_turn(self, record: TurnRecord) -> None:
        await self._execute(
            "insert into session_turns"
            " (tenant, session_id, generation, turn_number, turn_id, started_at, ended_at,"
            "  duration_ms, end_reason) values ($1,$2,$3,$4,$5,$6,$7,$8,$9)"
            " on conflict (tenant, session_id, generation, turn_number) do update set"
            "   turn_id = coalesce(excluded.turn_id, session_turns.turn_id),"
            "   started_at = coalesce(excluded.started_at, session_turns.started_at),"
            "   ended_at = coalesce(excluded.ended_at, session_turns.ended_at),"
            "   duration_ms = coalesce(excluded.duration_ms, session_turns.duration_ms),"
            "   end_reason = coalesce(excluded.end_reason, session_turns.end_reason)",
            record.tenant,
            record.session_id,
            record.generation,
            record.turn_number,
            record.turn_id,
            record.started_at,
            record.ended_at,
            record.duration_ms,
            record.end_reason,
        )

    async def list_turns(self, tenant: str, session_id: str) -> list[TurnRecord]:
        rows = await self._fetch(
            "select tenant, session_id, generation, turn_number, turn_id, started_at, ended_at,"
            " duration_ms, end_reason from session_turns"
            " where tenant = $1 and session_id = $2 order by generation, turn_number",
            tenant,
            session_id,
        )
        return [TurnRecord(**dict(row)) for row in rows]

    # 事件

    async def put_batch(self, record: BatchRecord) -> None:
        await self._execute(
            "insert into event_batches"
            " (tenant, session_id, batch_id, generation, client_sequence, first_event_sequence,"
            "  last_event_sequence, event_count, unparsed_count, gap_count, completeness_state,"
            "  payload_sha256, payload_size, payload_etag)"
            " values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)"
            " on conflict (tenant, batch_id) do nothing",
            record.tenant,
            record.session_id,
            record.batch_id,
            record.generation,
            record.client_sequence,
            record.first_event_sequence,
            record.last_event_sequence,
            record.event_count,
            record.unparsed_count,
            record.gap_count,
            record.completeness_state,
            record.payload_sha256,
            record.payload_size,
            record.payload_etag,
        )

    async def put_events(self, records: Sequence[EventRecord]) -> None:
        if not records:
            return
        await self._executemany(
            "insert into trajectory_events"
            " (tenant, session_id, generation, sequence, batch_id, event_id, event_type, turn_id,"
            "  operation_id, actor_kind, actor_role, occurred_at, monotonic_ns, tool_name,"
            "  tool_kind,"
            "  duration_ms, outcome, output_tokens, input_tokens)"
            " values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)"
            " on conflict (tenant, session_id, generation, sequence) do nothing",
            [
                (
                    row.tenant,
                    row.session_id,
                    row.generation,
                    row.sequence,
                    row.batch_id,
                    row.event_id,
                    row.event_type,
                    row.turn_id,
                    row.operation_id,
                    row.actor_kind,
                    row.actor_role,
                    row.occurred_at,
                    row.monotonic_ns,
                    row.tool_name,
                    row.tool_kind,
                    row.duration_ms,
                    row.outcome,
                    row.output_tokens,
                    row.input_tokens,
                )
                for row in records
            ],
        )

    async def max_committed_sequence(self, tenant: str, session_id: str) -> int:
        value = await self._fetchval(
            "select coalesce(max(sequence), 0) from trajectory_events"
            " where tenant = $1 and session_id = $2",
            tenant,
            session_id,
        )
        return int(value or 0)

    async def list_events(self, tenant: str, session_id: str) -> list[EventRecord]:
        rows = await self._fetch(
            "select tenant, session_id, generation, sequence, batch_id, event_id, event_type,"
            " turn_id, operation_id, actor_kind, actor_role, occurred_at, monotonic_ns, tool_name,"
            " tool_kind, duration_ms, outcome, output_tokens, input_tokens"
            " from trajectory_events where tenant = $1 and session_id = $2"
            " order by generation, sequence",
            tenant,
            session_id,
        )
        return [EventRecord(**dict(row)) for row in rows]

    # 工作区

    async def upsert_workspace_state(self, record: WorkspaceStateRecord) -> None:
        await self._execute(
            "insert into workspace_states"
            " (tenant, external_session_id, repository_identity, user_id, head_sha, head_tree_sha,"
            "  branch, detached, remote_kind, staged_entries, worktree_entries, untracked_entries,"
            "  omitted_count, complete, session_started_at, captured_at, capture_duration_ms,"
            "  body_sha256, body_size_bytes, body_etag, ignored_count, ignored_truncated,"
            "  ignored_entries)"
            " values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,"
            " $21,$22,$23::jsonb)"
            " on conflict (tenant, external_session_id, repository_identity) do update set"
            "   captured_at = excluded.captured_at,"
            "   body_sha256 = excluded.body_sha256,"
            "   body_size_bytes = excluded.body_size_bytes,"
            "   head_sha = excluded.head_sha,"
            "   head_tree_sha = excluded.head_tree_sha,"
            "   branch = excluded.branch,"
            "   detached = excluded.detached,"
            "   remote_kind = excluded.remote_kind,"
            "   staged_entries = excluded.staged_entries,"
            "   worktree_entries = excluded.worktree_entries,"
            "   untracked_entries = excluded.untracked_entries,"
            "   omitted_count = excluded.omitted_count,"
            "   complete = excluded.complete,"
            "   capture_duration_ms = excluded.capture_duration_ms"
            " where workspace_states.captured_at is null"
            "    or excluded.captured_at >= workspace_states.captured_at",
            record.tenant,
            record.session_id,
            record.repository_identity,
            record.user_id,
            record.head_sha,
            record.head_tree_sha,
            record.branch,
            record.detached,
            record.remote_kind,
            record.staged_entries,
            record.worktree_entries,
            record.untracked_entries,
            record.omitted_count,
            record.complete,
            record.session_started_at,
            record.captured_at,
            record.capture_duration_ms,
            record.body_sha256,
            record.body_size_bytes,
            record.body_etag,
            record.ignored_count,
            record.ignored_truncated,
            json.dumps(list(record.ignored_entries)),
        )

    async def list_workspace_states(
        self, tenant: str, session_id: str
    ) -> list[WorkspaceStateRecord]:
        rows = await self._fetch(
            "select * from workspace_states where tenant = $1 and external_session_id = $2",
            tenant,
            session_id,
        )
        records: list[WorkspaceStateRecord] = []
        for row in rows:
            data = dict(row)
            raw_ignored = data.get("ignored_entries") or []
            if isinstance(raw_ignored, str):
                raw_ignored = json.loads(raw_ignored)
            records.append(
                WorkspaceStateRecord(
                    tenant=data["tenant"],
                    session_id=data["external_session_id"],
                    repository_identity=data["repository_identity"],
                    user_id=data["user_id"],
                    remote_kind=data["remote_kind"],
                    body_sha256=data["body_sha256"],
                    body_size_bytes=data["body_size_bytes"],
                    head_sha=data["head_sha"],
                    head_tree_sha=data["head_tree_sha"],
                    branch=data["branch"],
                    detached=data["detached"],
                    staged_entries=data["staged_entries"],
                    worktree_entries=data["worktree_entries"],
                    untracked_entries=data["untracked_entries"],
                    omitted_count=data["omitted_count"],
                    complete=data["complete"],
                    session_started_at=data["session_started_at"],
                    captured_at=data["captured_at"],
                    capture_duration_ms=data["capture_duration_ms"],
                    ignored_count=data["ignored_count"],
                    ignored_truncated=data["ignored_truncated"],
                    ignored_entries=tuple(raw_ignored),
                    body_etag=data["body_etag"],
                )
            )
        return records

    # 仓库登记

    async def upsert_repository(self, record: RepositoryRecord) -> None:
        await self._execute(
            "insert into repository_registry"
            " (tenant, local_identity, repository_key, provider, connection_id, host, api_path,"
            "  resolution, user_id, first_seen_at, last_seen_at, observed_at)"
            " values ($1,$2,$3,$4,$5,$6,$7,$8,$9, now(), now(), $10)"
            " on conflict (tenant, local_identity) do update set"
            "   repository_key = excluded.repository_key,"
            "   provider = excluded.provider,"
            "   host = excluded.host,"
            "   api_path = excluded.api_path,"
            "   resolution = excluded.resolution,"
            "   user_id = excluded.user_id,"
            "   last_seen_at = now(),"
            "   observed_at = excluded.observed_at",
            record.tenant,
            record.local_identity,
            record.repository_key,
            record.provider,
            record.connection_id,
            record.host,
            record.api_path,
            record.resolution,
            record.user_id,
            record.observed_at,
        )

    async def list_repositories(self, tenant: str) -> list[RepositoryRecord]:
        rows = await self._fetch(
            "select tenant, local_identity, repository_key, provider, connection_id, host,"
            " api_path, resolution, user_id, observed_at, first_seen_at, last_seen_at"
            " from repository_registry where tenant = $1",
            tenant,
        )
        return [RepositoryRecord(**dict(row)) for row in rows]

    # 低层助手

    def _active(self) -> asyncpg.Connection | None:
        return _CONNECTION.get()

    async def _fetchrow(self, query: str, *args: object):
        connection = self._active()
        if connection is not None:
            return await connection.fetchrow(query, *args)
        assert self._pool is not None, "repository is not started"
        async with self._pool.acquire() as pooled:
            return await pooled.fetchrow(query, *args)

    async def _fetch(self, query: str, *args: object):
        connection = self._active()
        if connection is not None:
            return await connection.fetch(query, *args)
        assert self._pool is not None, "repository is not started"
        async with self._pool.acquire() as pooled:
            return await pooled.fetch(query, *args)

    async def _fetchval(self, query: str, *args: object):
        connection = self._active()
        if connection is not None:
            return await connection.fetchval(query, *args)
        assert self._pool is not None, "repository is not started"
        async with self._pool.acquire() as pooled:
            return await pooled.fetchval(query, *args)

    async def _execute(self, query: str, *args: object) -> str:
        connection = self._active()
        if connection is not None:
            return await connection.execute(query, *args)
        assert self._pool is not None, "repository is not started"
        async with self._pool.acquire() as pooled:
            return await pooled.execute(query, *args)

    async def _executemany(self, query: str, args: Sequence[tuple]) -> None:
        connection = self._active()
        if connection is not None:
            await connection.executemany(query, args)
            return
        assert self._pool is not None, "repository is not started"
        async with self._pool.acquire() as pooled:
            await pooled.executemany(query, args)


def _object_from_row(row) -> ObjectRecord:
    data = dict(row)
    return ObjectRecord(
        tenant=data["tenant"],
        sha256=data["sha256"],
        size_bytes=data["size_bytes"],
        channel=data["channel"],
        state=data["state"],
        etag=data["etag"],
        missing_hashes=tuple(data.get("missing_hashes") or ()),
        created_at=data["created_at"],
    )


def _session_from_row(row) -> SessionRecord:
    data = dict(row)
    return SessionRecord(
        tenant=data["tenant"],
        session_id=data["external_session_id"],
        user_id=data["user_id"],
        content_hash=data["content_hash"],
        observed_at=data["observed_at"],
        updated_at=data["updated_at"],
        created_at=data["created_at"],
        title=data["title"],
        agent_profile=data["agent_profile"],
        model_provider=data["model_provider"],
        model_id=data["model_id"],
        producer_version=data["producer_version"],
        parent_session_id=data["parent_external_session_id"],
        primary_cwd_digest=data["primary_cwd_digest"],
        turn_count=data["turn_count"],
        message_count=data["message_count"],
        total_tokens=data["total_tokens"],
        repository_identities=tuple(data.get("repository_identities") or ()),
        snapshot_sha256=data["snapshot_sha256"],
        snapshot_size=data["snapshot_size"],
        snapshot_state=data["snapshot_state"],
        body_state=data["body_state"],
        agent_config_sha256=data["agent_config_sha256"],
        workspace_kind=data["workspace_kind"],
        agent_config_captured_at=data["agent_config_captured_at"],
    )


def _is_newer(
    updated_at: datetime | None,
    observed_at: datetime | None,
    previous_updated: datetime | None,
    previous_observed: datetime | None,
) -> bool:
    epoch = datetime.min.replace(tzinfo=UTC)
    left = (updated_at or epoch, observed_at or epoch)
    right = (previous_updated or epoch, previous_observed or epoch)
    return left > right
