"""轨迹行缓冲与切窗持久化（server-acceptance §3.3 / §3.4）。

客户端只搬原样字节，服务端是切窗、索引抽取、代际派生的唯一实现方（决策 D2）：

1. 以 `(session_id, sequence)` 为行的位置键：相同内容去重，不同内容记 `sequence_conflict`；
2. `reset=true` 的批次按行内容判定：纯重复 → 代际不变；同 sequence 不同内容 → 文件重建 → 代际 +1；
3. 有序缓冲上跑纯函数派生器（`derive.py`），每切出一个窗口就写 OBS + `event_batches` /
   `trajectory_events` / `session_turns`。

进程内缓冲是「未切窗的行 + 各代际已见行摘要」；权威事实都在 OBS 与 PG 里。
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field

from ..errors import Outcome, RejectionCode
from ..storage.models import BatchRecord, EventRecord, ObjectRecord, TurnRecord
from ..storage.objects import ObjectStore
from ..storage.repository import Repository
from .derive import (
    MAX_INDEX_LINES,
    MAX_WINDOW_BYTES,
    DerivationState,
    DerivedWindow,
    TrajectoryLine,
    derive_windows,
    split_payload,
)
from .locks import KeyedLocks
from .session_meta import parse_rfc3339

logger = logging.getLogger(__name__)

WINDOW_CHANNEL = "event-window"


@dataclass
class SessionTrajectory:
    generation: int = 0
    branch_id: str | None = None
    next_sequence: int = 0
    gaps: tuple[tuple[int, int], ...] = ()
    pending: dict[int, TrajectoryLine] = field(default_factory=dict)
    # generation -> {sequence: 行内容摘要}：同 sequence 同内容去重、不同内容判冲突/重建
    seen: dict[int, dict[int, str]] = field(default_factory=dict)
    max_applied_sequence: int = 0


class TrajectoryStore:
    def __init__(
        self,
        repository: Repository,
        objects: ObjectStore,
        *,
        max_index_lines: int = MAX_INDEX_LINES,
        max_window_bytes: int = MAX_WINDOW_BYTES,
    ) -> None:
        self.repository = repository
        self.objects = objects
        self._max_index_lines = max_index_lines
        self._max_window_bytes = max_window_bytes
        self._sessions: dict[tuple[str, str], SessionTrajectory] = {}
        self._locks = KeyedLocks()

    def state_for(self, tenant: str, session_id: str) -> SessionTrajectory:
        return self._sessions.setdefault((tenant, session_id), SessionTrajectory())

    async def _ensure_state(self, tenant: str, session_id: str) -> SessionTrajectory:
        """首次触碰会话时从已提交行恢复水位：乱序首帧不推进窗口（§3.5-1 / T10）。"""
        key = (tenant, session_id)
        state = self._sessions.get(key)
        if state is not None:
            return state
        state = SessionTrajectory(next_sequence=1)
        events = await self.repository.list_events(tenant, session_id)
        if events:
            last = max(events, key=lambda row: (row.generation, row.sequence))
            state.generation = last.generation
            state.next_sequence = last.sequence + 1
            state.max_applied_sequence = max(row.sequence for row in events)
        self._sessions[key] = state
        return state

    async def apply_lines(
        self, *, tenant: str, session_id: str, payload: bytes, reset: bool
    ) -> Outcome:
        try:
            lines = split_payload(payload)
        except ValueError as exc:
            return Outcome.rejected(RejectionCode.SCHEMA_INVALID, str(exc))
        if not lines:
            return Outcome.rejected(RejectionCode.SCHEMA_INVALID, "trajectory.lines is empty")

        async with self._locks.hold((tenant, session_id)):
            state = await self._ensure_state(tenant, session_id)
            digest_of = {line.sequence: hashlib.sha256(line.raw).hexdigest() for line in lines}
            seen = state.seen.setdefault(state.generation, {})
            conflict = any(
                sequence in seen and seen[sequence] != digest
                for sequence, digest in digest_of.items()
            )
            fresh = [line for line in lines if seen.get(line.sequence) != digest_of[line.sequence]]
            if conflict:
                if not reset:
                    return Outcome.rejected(
                        RejectionCode.SEQUENCE_CONFLICT,
                        "same sequence with different content; existing rows are kept",
                    )
                # 文件重建（§3.3-5）：代际 +1，旧代际的行与窗口原样保留。
                state.generation += 1
                state.branch_id = None
                state.next_sequence = 0
                state.pending.clear()
                seen = state.seen.setdefault(state.generation, {})
                fresh = list(lines)
            if not fresh:
                return Outcome.duplicate()

            for line in fresh:
                seen[line.sequence] = digest_of[line.sequence]
                state.pending[line.sequence] = line
                state.max_applied_sequence = max(state.max_applied_sequence, line.sequence)

            await self._derive_and_commit(tenant, session_id, state)
            return Outcome.committed()

    async def _derive_and_commit(
        self, tenant: str, session_id: str, state: SessionTrajectory
    ) -> None:
        ordered = [state.pending[sequence] for sequence in sorted(state.pending)]
        if not ordered:
            return
        derivation = derive_windows(
            ordered,
            state=DerivationState(
                generation=state.generation,
                branch_id=state.branch_id,
                next_sequence=state.next_sequence,
            ),
            gaps=state.gaps,
            max_index_lines=self._max_index_lines,
            max_window_bytes=self._max_window_bytes,
        )
        for window in derivation.windows:
            await self._commit_window(tenant, session_id, window)
        for line in ordered[: derivation.consumed]:
            state.pending.pop(line.sequence, None)
        # 未闭合的尾部保留在 pending；水位与代际只推进到最后一个已切出的窗口。
        state.generation = derivation.state.generation
        state.branch_id = derivation.state.branch_id
        state.next_sequence = derivation.state.next_sequence
        state.gaps = derivation.gaps

    async def _commit_window(self, tenant: str, session_id: str, window: DerivedWindow) -> None:
        await self.objects.put(tenant, window.payload_sha256, window.payload)
        await self.repository.put_object(
            ObjectRecord(
                tenant=tenant,
                sha256=window.payload_sha256,
                size_bytes=len(window.payload),
                channel=WINDOW_CHANNEL,
                state="verified",
            )
        )
        await self.repository.put_batch(
            BatchRecord(
                tenant=tenant,
                session_id=session_id,
                batch_id=window.payload_sha256,
                generation=window.generation,
                client_sequence=window.last_sequence,
                first_event_sequence=window.first_sequence,
                last_event_sequence=window.last_sequence,
                event_count=window.event_count,
                unparsed_count=window.unparsed_count,
                gap_count=window.gap_count,
                completeness_state=window.completeness,
                payload_sha256=window.payload_sha256,
                payload_size=len(window.payload),
            )
        )
        await self.repository.put_events(
            [
                EventRecord(
                    tenant=tenant,
                    session_id=session_id,
                    generation=window.generation,
                    sequence=row.sequence,
                    batch_id=window.payload_sha256,
                    event_id=row.event_id,
                    event_type=row.event_type,
                    turn_id=row.turn_id,
                    operation_id=row.operation_id,
                    actor_kind=row.actor_kind,
                    actor_role=row.actor_role,
                    occurred_at=parse_rfc3339(row.occurred_at),
                    monotonic_ns=row.monotonic_ns,
                    tool_name=row.tool_name,
                    tool_kind=row.tool_kind,
                    duration_ms=row.duration_ms,
                    outcome=row.outcome,
                    output_tokens=row.output_tokens,
                    input_tokens=row.input_tokens,
                )
                for row in window.events
            ]
        )
        for turn in window.turns:
            await self.repository.upsert_turn(
                TurnRecord(
                    tenant=tenant,
                    session_id=session_id,
                    generation=window.generation,
                    turn_number=turn.turn_number,
                    turn_id=turn.turn_id,
                    started_at=parse_rfc3339(turn.started_at),
                    ended_at=parse_rfc3339(turn.ended_at),
                    duration_ms=turn.duration_ms,
                    end_reason=turn.end_reason,
                )
            )

    def applied_sequence(self, tenant: str, session_id: str) -> int:
        state = self._sessions.get((tenant, session_id))
        return state.max_applied_sequence if state else 0

    def pending_count(self, tenant: str, session_id: str) -> int:
        state = self._sessions.get((tenant, session_id))
        return len(state.pending) if state else 0
