"""仓储抽象与内存实现（测试/开发默认）。

`Repository` 只承载 teams schema v10 在范围内表的读写（server-acceptance §4.2）；
`PostgresRepository` 是生产实现，`MemoryRepository` 是开发/测试默认，保证 `uv run` 零外部依赖。
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Protocol

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


def NOW() -> datetime:
    return datetime.now(UTC)


def _anchor_key(record: SessionRecord) -> tuple[datetime, datetime]:
    epoch = datetime.min.replace(tzinfo=UTC)
    return (record.updated_at or epoch, record.observed_at or epoch)


class Repository(Protocol):
    async def start(self) -> None: ...

    async def close(self) -> None: ...

    def transaction(self) -> AsyncIterator[None]: ...

    # 幂等收据（ingest_receipts）
    async def get_receipt(self, tenant: str, unit_id: str) -> ReceiptRecord | None: ...

    async def put_receipt(self, record: ReceiptRecord) -> bool: ...

    # 对象登记（object_registry）
    async def put_object(self, record: ObjectRecord) -> None: ...

    async def get_object(self, tenant: str, sha256: str) -> ObjectRecord | None: ...

    async def verified_objects(self, tenant: str, shas: Sequence[str]) -> set[str]: ...

    # 会话（sessions）
    async def get_session(self, tenant: str, session_id: str) -> SessionRecord | None: ...

    async def upsert_session(self, record: SessionRecord) -> bool: ...

    async def set_agent_config_anchor(self, tenant: str, session_id: str, sha256: str) -> None: ...

    async def get_agent_config_anchor(self, tenant: str, session_id: str) -> str | None: ...

    async def set_session_body_state(self, tenant: str, session_id: str, state: str) -> None: ...

    # 轮次（session_turns）
    async def upsert_turn(self, record: TurnRecord) -> None: ...

    async def list_turns(self, tenant: str, session_id: str) -> list[TurnRecord]: ...

    # 事件（event_batches / trajectory_events）
    async def put_batch(self, record: BatchRecord) -> None: ...

    async def put_events(self, records: Sequence[EventRecord]) -> None: ...

    async def max_committed_sequence(self, tenant: str, session_id: str) -> int: ...

    async def list_events(self, tenant: str, session_id: str) -> list[EventRecord]: ...

    # 工作区（workspace_states）
    async def upsert_workspace_state(self, record: WorkspaceStateRecord) -> None: ...

    async def list_workspace_states(
        self, tenant: str, session_id: str
    ) -> list[WorkspaceStateRecord]: ...

    # 仓库登记（repository_registry）
    async def upsert_repository(self, record: RepositoryRecord) -> None: ...

    async def list_repositories(self, tenant: str) -> list[RepositoryRecord]: ...


class MemoryRepository:
    """进程内实现；`transaction()` 用写时快照模拟提交/回滚。"""

    def __init__(self) -> None:
        self.receipts: dict[tuple[str, str], ReceiptRecord] = {}
        self.objects: dict[tuple[str, str], ObjectRecord] = {}
        self.sessions: dict[tuple[str, str], SessionRecord] = {}
        self.anchors: dict[tuple[str, str], SessionAnchor] = {}
        self.turns: dict[tuple[str, str, int, int], TurnRecord] = {}
        self.batches: dict[tuple[str, str], BatchRecord] = {}
        self.events: dict[tuple[str, str, int, int], EventRecord] = {}
        self.workspaces: dict[tuple[str, str, str], WorkspaceStateRecord] = {}
        self.repositories: dict[tuple[str, str], RepositoryRecord] = {}
        self._lock = asyncio.Lock()
        self._owner: asyncio.Task | None = None
        self._depth = 0
        self._snapshot: tuple | None = None
        self._stores = (
            "receipts",
            "objects",
            "sessions",
            "anchors",
            "turns",
            "batches",
            "events",
            "workspaces",
            "repositories",
        )

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        # 可重入：同一任务内的嵌套事务复用最外层事务；不同任务串行化（单进程的写入串行化语义）。
        task = asyncio.current_task()
        owning = self._depth > 0 and self._owner is task
        if not owning:
            await self._lock.acquire()
            self._owner = task
        if self._depth == 0:
            self._snapshot = tuple(dict(getattr(self, name)) for name in self._stores)
        self._depth += 1
        try:
            yield
        except BaseException:
            self._depth -= 1
            if self._depth == 0:
                if self._snapshot is not None:
                    for name, saved in zip(self._stores, self._snapshot, strict=True):
                        setattr(self, name, dict(saved))
                    self._snapshot = None
                self._owner = None
                self._lock.release()
            raise
        else:
            self._depth -= 1
            if self._depth == 0:
                self._snapshot = None
                self._owner = None
                self._lock.release()

    # 幂等收据

    async def get_receipt(self, tenant: str, unit_id: str) -> ReceiptRecord | None:
        return self.receipts.get((tenant, unit_id))

    async def put_receipt(self, record: ReceiptRecord) -> bool:
        key = (record.tenant, record.unit_id)
        existing = self.receipts.get(key)
        if existing is not None:
            return False
        self.receipts[key] = ReceiptRecord(
            tenant=record.tenant,
            unit_id=record.unit_id,
            sha256=record.sha256,
            state=record.state,
            channel=record.channel,
            code=record.code,
            message=record.message,
            created_at=record.created_at or NOW(),
        )
        return True

    # 对象登记

    async def put_object(self, record: ObjectRecord) -> None:
        self.objects[(record.tenant, record.sha256)] = record

    async def get_object(self, tenant: str, sha256: str) -> ObjectRecord | None:
        return self.objects.get((tenant, sha256))

    async def verified_objects(self, tenant: str, shas: Sequence[str]) -> set[str]:
        return {
            sha
            for sha in shas
            if (record := self.objects.get((tenant, sha))) is not None
            and record.state == "verified"
        }

    # 会话

    async def get_session(self, tenant: str, session_id: str) -> SessionRecord | None:
        return self.sessions.get((tenant, session_id))

    async def upsert_session(self, record: SessionRecord) -> bool:
        key = (record.tenant, record.session_id)
        existing = self.sessions.get(key)
        if existing is None:
            merged = record
            if merged.agent_config_sha256 is None:
                anchor = self.anchors.get(key)
                if anchor is not None:
                    merged = merged.with_updates(agent_config_sha256=anchor.agent_config_sha256)
            self.sessions[key] = merged
            return True
        if _anchor_key(record) <= _anchor_key(existing):
            return False
        identities: tuple[str, ...] = existing.repository_identities
        for identity in record.repository_identities:
            if identity not in identities:
                identities = (*identities, identity)[:16]
        merged = record.with_updates(
            repository_identities=identities,
            agent_config_sha256=record.agent_config_sha256 or existing.agent_config_sha256,
            created_at=record.created_at or existing.created_at,
        )
        self.sessions[key] = merged
        return True

    async def set_agent_config_anchor(self, tenant: str, session_id: str, sha256: str) -> None:
        key = (tenant, session_id)
        previous = self.anchors.get(key)
        self.anchors[key] = SessionAnchor(
            tenant=tenant,
            session_id=session_id,
            agent_config_sha256=sha256,
            workspace_kind=previous.workspace_kind if previous else None,
            updated_at=NOW(),
        )
        session = self.sessions.get(key)
        if session is not None and session.agent_config_sha256 != sha256:
            self.sessions[key] = session.with_updates(agent_config_sha256=sha256)

    async def get_agent_config_anchor(self, tenant: str, session_id: str) -> str | None:
        session = self.sessions.get((tenant, session_id))
        if session is not None and session.agent_config_sha256:
            return session.agent_config_sha256
        anchor = self.anchors.get((tenant, session_id))
        return anchor.agent_config_sha256 if anchor else None

    async def set_session_body_state(self, tenant: str, session_id: str, state: str) -> None:
        key = (tenant, session_id)
        session = self.sessions.get(key)
        if session is not None:
            self.sessions[key] = session.with_updates(body_state=state)

    # 轮次

    async def upsert_turn(self, record: TurnRecord) -> None:
        key = (record.tenant, record.session_id, record.generation, record.turn_number)
        existing = self.turns.get(key)
        if existing is None:
            self.turns[key] = record
            return
        self.turns[key] = TurnRecord(
            tenant=record.tenant,
            session_id=record.session_id,
            generation=record.generation,
            turn_number=record.turn_number,
            turn_id=record.turn_id or existing.turn_id,
            started_at=record.started_at or existing.started_at,
            ended_at=record.ended_at or existing.ended_at,
            duration_ms=record.duration_ms
            if record.duration_ms is not None
            else existing.duration_ms,
            end_reason=record.end_reason or existing.end_reason,
        )

    async def list_turns(self, tenant: str, session_id: str) -> list[TurnRecord]:
        rows = [
            row for (t, s, _g, _n), row in self.turns.items() if t == tenant and s == session_id
        ]
        return sorted(rows, key=lambda row: (row.generation, row.turn_number))

    # 事件

    async def put_batch(self, record: BatchRecord) -> None:
        self.batches.setdefault((record.tenant, record.batch_id), record)

    async def put_events(self, records: Sequence[EventRecord]) -> None:
        for record in records:
            key = (record.tenant, record.session_id, record.generation, record.sequence)
            self.events.setdefault(key, record)

    async def max_committed_sequence(self, tenant: str, session_id: str) -> int:
        sequences = [
            sequence for (t, s, _g, sequence) in self.events if t == tenant and s == session_id
        ]
        return max(sequences, default=0)

    async def list_events(self, tenant: str, session_id: str) -> list[EventRecord]:
        rows = [
            row for (t, s, _g, _seq), row in self.events.items() if t == tenant and s == session_id
        ]
        return sorted(rows, key=lambda row: (row.generation, row.sequence))

    # 工作区

    async def upsert_workspace_state(self, record: WorkspaceStateRecord) -> None:
        key = (record.tenant, record.session_id, record.repository_identity)
        existing = self.workspaces.get(key)
        if existing is not None and _dt(existing.captured_at) > _dt(record.captured_at):
            return
        self.workspaces[key] = record

    async def list_workspace_states(
        self, tenant: str, session_id: str
    ) -> list[WorkspaceStateRecord]:
        return [
            row for (t, s, _r), row in self.workspaces.items() if t == tenant and s == session_id
        ]

    # 仓库登记

    async def upsert_repository(self, record: RepositoryRecord) -> None:
        key = (record.tenant, record.local_identity)
        existing = self.repositories.get(key)
        if existing is not None:
            record = RepositoryRecord(
                tenant=record.tenant,
                local_identity=record.local_identity,
                repository_key=record.repository_key,
                provider=record.provider,
                connection_id=record.connection_id,
                host=record.host,
                api_path=record.api_path,
                resolution=record.resolution,
                user_id=record.user_id,
                observed_at=record.observed_at,
                first_seen_at=existing.first_seen_at,
                last_seen_at=record.observed_at,
            )
        else:
            record = RepositoryRecord(
                tenant=record.tenant,
                local_identity=record.local_identity,
                repository_key=record.repository_key,
                provider=record.provider,
                connection_id=record.connection_id,
                host=record.host,
                api_path=record.api_path,
                resolution=record.resolution,
                user_id=record.user_id,
                observed_at=record.observed_at,
                first_seen_at=record.observed_at,
                last_seen_at=record.observed_at,
            )
        self.repositories[key] = record

    async def list_repositories(self, tenant: str) -> list[RepositoryRecord]:
        return [row for (t, _id), row in self.repositories.items() if t == tenant]


def _dt(value: datetime | None) -> datetime:
    return value or datetime.min.replace(tzinfo=UTC)


__all__ = [
    "MemoryRepository",
    "Repository",
]
