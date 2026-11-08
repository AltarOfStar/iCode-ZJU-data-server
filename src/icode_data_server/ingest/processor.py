"""单元分发与批处理（server-acceptance §2.2 / §3）。

每个单元的执行顺序（§4.3）：

    ingest_receipts(pending) → 组装校验 / 写 OBS → 目标表写入 → committed

任一步失败 → 事务回滚、收据保留 pending、响应不放 `accepted`（ACK-after-commit）。
`deferred` 语义上"无副作用"，实现上通过回滚事务保证。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

from ..config import Principal, Settings
from ..errors import Outcome, OutcomeKind, RejectionCode, RollbackUnit
from ..frames import FRAME_VERSION, Frame, unit_id_for
from ..storage.models import (
    OBJECT_MISSING,
    OBJECT_VERIFIED,
    RECEIPT_COMMITTED,
    RECEIPT_REJECTED,
    RECEIPT_UNSUPPORTED,
    TERMINAL_RECEIPT_STATES,
    ObjectRecord,
    ReceiptRecord,
    RepositoryRecord,
    SessionRecord,
    WorkspaceStateRecord,
)
from ..storage.objects import ObjectStore
from ..storage.repository import Repository
from .agent_config import WORKSPACE_KINDS, AgentConfigError, parse_agent_config
from .assembly import WORKSPACE_FILE, AssemblyStore
from .backpressure import TenantGuard, TokenBucket
from .derive import parse_line
from .locks import KeyedLocks
from .repository_identity import parse_repository_identity
from .session_meta import (
    SessionMetaError,
    parse_rfc3339,
    parse_session_meta,
)
from .snapshots import SNAPSHOT_CHANNEL, SNAPSHOT_CHUNK_CHANNEL, SnapshotAssembler
from .trajectory import TrajectoryStore
from .workspace import (
    WorkspaceManifestError,
    build_container,
    container_sha256,
    content_refs,
    head_fields,
    ignored_summary,
    parse_manifest,
    remote_kind,
    repository_identity,
    summary_counts,
)

logger = logging.getLogger(__name__)

KIND_TRAJECTORY_LINES = "trajectory.lines"
KIND_SNAPSHOT_CHUNK = "snapshot.chunk"
KIND_SNAPSHOT_MANIFEST = "snapshot.manifest"
KIND_SESSION_META = "session.meta"
KIND_WORKSPACE_MANIFEST = "workspace.manifest"
KIND_WORKSPACE_FILE = "workspace.file"
KIND_AGENT_ENV = "agent.env"
KIND_REPO_IDENTITY = "repo.identity"

KNOWN_KINDS = frozenset(
    {
        KIND_TRAJECTORY_LINES,
        KIND_SNAPSHOT_CHUNK,
        KIND_SNAPSHOT_MANIFEST,
        KIND_SESSION_META,
        KIND_WORKSPACE_MANIFEST,
        KIND_WORKSPACE_FILE,
        KIND_AGENT_ENV,
        KIND_REPO_IDENTITY,
    }
)

# 这些单元的重放允许"重新物化"中间态：分片/清单可能因重启或 TTL 释放而丢失，
# 而它们的收据已 committed，客户端不会再补发。副作用幂等（同一内容 → 同一
# 对象/分片），不产生新的持久事实，响应仍是 duplicates（§2.2）。
_REDELIVER_KINDS = frozenset(
    {
        KIND_SNAPSHOT_CHUNK,
        KIND_SNAPSHOT_MANIFEST,
        KIND_WORKSPACE_FILE,
    }
)

_SHA = r"(?P<sha>[0-9a-f]{64})"
SCOPE_TRAJECTORY = re.compile(r"^lines:(?P<first>\d+)-(?P<last>\d+)$")
SCOPE_SNAPSHOT_CHUNK = re.compile(rf"^chunk:{_SHA}:(?P<index>\d+)$")
SCOPE_SNAPSHOT_MANIFEST = re.compile(rf"^manifest:{_SHA}$")
SCOPE_SESSION_META = re.compile(rf"^meta:{_SHA}$")
SCOPE_WORKSPACE_MANIFEST = re.compile(rf"^ws:{_SHA}$")
SCOPE_WORKSPACE_FILE = re.compile(rf"^wsfile:{_SHA}:(?P<index>\d+)$")
SCOPE_AGENT_ENV = re.compile(rf"^agentenv:{_SHA}$")
SCOPE_REPOSITORY = re.compile(r"^repo:(?P<identity>.+)$")

QUARANTINE_CHANNEL = "quarantine"


@dataclass(frozen=True, slots=True)
class Rejection:
    unit_id: str
    code: str
    message: str


@dataclass
class BatchResult:
    accepted: list[str] = field(default_factory=list)
    duplicates: list[str] = field(default_factory=list)
    rejected: list[Rejection] = field(default_factory=list)
    deferred: list[str] = field(default_factory=list)
    retry_after_ms: int = 0

    def as_response(self) -> dict:
        return {
            "accepted": self.accepted,
            "duplicates": self.duplicates,
            "rejected": [
                {"unit_id": item.unit_id, "code": item.code, "message": item.message}
                for item in self.rejected
            ],
            "deferred": self.deferred,
            "retry_after_ms": self.retry_after_ms,
        }


@dataclass(frozen=True, slots=True)
class UnitContext:
    principal: Principal
    frame: Frame
    payload: bytes

    @property
    def tenant(self) -> str:
        return self.principal.tenant

    @property
    def user_id(self) -> str:
        return self.principal.user_id

    @property
    def session_id(self) -> str:
        return self.frame.header.session_id

    @property
    def scope(self) -> str:
        return self.frame.header.scope

    @property
    def unit_id(self) -> str:
        return self.frame.header.unit_id


class BatchProcessor:
    def __init__(
        self,
        *,
        settings: Settings,
        repository: Repository,
        objects: ObjectStore,
        assemblies: AssemblyStore,
        trajectory: TrajectoryStore,
        assembler: SnapshotAssembler,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.objects = objects
        self.assemblies = assemblies
        self.trajectory = trajectory
        self.assembler = assembler
        self.user_limiter = TokenBucket(
            rate_per_second=settings.rate_limit_per_user_per_second,
            burst=settings.rate_limit_per_user_burst,
        )
        self.tenant_limiter = TokenBucket(
            rate_per_second=settings.rate_limit_per_second,
            burst=settings.rate_limit_burst,
        )
        self.guard = TenantGuard(max_inflight=settings.max_inflight_batches_per_tenant)
        self._unit_locks = KeyedLocks()
        self._session_locks = KeyedLocks()
        self._last_sweep = 0.0
        self._handlers = {
            KIND_TRAJECTORY_LINES: self._handle_trajectory_lines,
            KIND_SNAPSHOT_CHUNK: self._handle_snapshot_chunk,
            KIND_SNAPSHOT_MANIFEST: self._handle_snapshot_manifest,
            KIND_SESSION_META: self._handle_session_meta,
            KIND_WORKSPACE_MANIFEST: self._handle_workspace_manifest,
            KIND_WORKSPACE_FILE: self._handle_workspace_file,
            KIND_AGENT_ENV: self._handle_agent_env,
            KIND_REPO_IDENTITY: self._handle_repo_identity,
        }

    # ── 批处理入口 ──────────────────────────────────────────────────────────

    def admit(self, principal: Principal) -> bool:
        """两级令牌桶（§6.4-2）：单用户桶防单点刷量，租户桶是总闸。"""
        if not self.user_limiter.take(principal.user_id):
            return False
        return self.tenant_limiter.take(principal.tenant)

    async def process(self, principal: Principal, frames: Sequence[Frame]) -> BatchResult:
        result = BatchResult()
        if not self.guard.enter(principal.tenant):
            # 单租户并发超限：整批 deferred（无副作用），客户端按退避重发。
            result.deferred.extend(frame.header.unit_id for frame in frames)
            result.retry_after_ms = self.settings.retry_after_ms
            logger.warning(
                "batch_deferred_backpressure",
                extra={"event": "batch_deferred_backpressure", "tenant": principal.tenant},
            )
            return result
        try:
            await self._maybe_sweep()
            for frame in frames:
                await self._process_frame(principal, frame, result)
        finally:
            self.guard.leave(principal.tenant)
        if result.deferred and not result.retry_after_ms:
            # deferred ⇒ 建议 retry_after：预算/前置未满足与整批背压同语义（§2.2）。
            result.retry_after_ms = self.settings.retry_after_ms
        return result

    async def _process_frame(self, principal: Principal, frame: Frame, result: BatchResult) -> None:
        header = frame.header
        unit_id = header.unit_id
        payload = frame.payload

        if header.v != FRAME_VERSION:
            await self._isolate(principal, frame, RECEIPT_UNSUPPORTED, "unsupported_version")
            result.rejected.append(
                Rejection(unit_id, RejectionCode.UNSUPPORTED_VERSION, f"frame version {header.v}")
            )
            return
        if header.kind not in KNOWN_KINDS:
            await self._isolate(principal, frame, RECEIPT_UNSUPPORTED, "unsupported_kind")
            result.rejected.append(
                Rejection(unit_id, RejectionCode.UNSUPPORTED_KIND, f"unknown kind {header.kind!r}")
            )
            return
        if len(payload) > self.settings.max_frame_payload_bytes:
            await self._record_rejection(
                principal,
                frame,
                RejectionCode.PAYLOAD_TOO_LARGE,
                f"frame payload {len(payload)} bytes",
            )
            result.rejected.append(
                Rejection(
                    unit_id,
                    RejectionCode.PAYLOAD_TOO_LARGE,
                    f"frame payload {len(payload)} bytes",
                )
            )
            return
        computed = unit_id_for(header.kind, header.session_id, header.scope, header.payload_sha256)
        if computed != unit_id:
            message = "unit_id does not match (kind, session_id, scope, payload_sha256)"
            await self._record_rejection(principal, frame, RejectionCode.SCHEMA_INVALID, message)
            result.rejected.append(Rejection(unit_id, RejectionCode.SCHEMA_INVALID, message))
            return
        actual_sha = hashlib.sha256(payload).hexdigest()
        if actual_sha != header.payload_sha256:
            message = "payload sha256 does not match the frame header"
            await self._record_rejection(
                principal, frame, RejectionCode.PAYLOAD_SHA_MISMATCH, message
            )
            result.rejected.append(Rejection(unit_id, RejectionCode.PAYLOAD_SHA_MISMATCH, message))
            return

        outcome = await self._run_unit(
            UnitContext(principal=principal, frame=frame, payload=payload)
        )
        if outcome.kind is OutcomeKind.DUPLICATE:
            result.duplicates.append(unit_id)
        elif outcome.kind is OutcomeKind.COMMITTED:
            result.accepted.append(unit_id)
        elif outcome.kind is OutcomeKind.DEFERRED:
            result.deferred.append(unit_id)
        else:
            result.rejected.append(Rejection(unit_id, str(outcome.code), outcome.message))

    async def _run_unit(self, context: UnitContext) -> Outcome:
        async with self._unit_locks.hold((context.tenant, context.unit_id)):
            existing = await self.repository.get_receipt(context.tenant, context.unit_id)
            if existing is not None and existing.state in TERMINAL_RECEIPT_STATES:
                if context.frame.header.kind == KIND_AGENT_ENV:
                    return await self._refresh_agent_env_envelope(context)
                if context.frame.header.kind not in _REDELIVER_KINDS:
                    return Outcome.duplicate()
                return await self._redeliver(context)
            handler = self._handlers[context.frame.header.kind]
            try:
                async with self.repository.transaction():
                    outcome = await handler(context)
                    if outcome.kind is OutcomeKind.DEFERRED:
                        raise RollbackUnit()
                    await self.repository.put_receipt(
                        ReceiptRecord(
                            tenant=context.tenant,
                            unit_id=context.unit_id,
                            sha256=context.frame.header.payload_sha256,
                            state=RECEIPT_COMMITTED
                            if outcome.kind is not OutcomeKind.REJECTED
                            else RECEIPT_REJECTED,
                            code=str(outcome.code) if outcome.code else None,
                            message=outcome.message or None,
                        )
                    )
            except RollbackUnit:
                logger.info(
                    "unit_deferred",
                    extra={
                        "event": "unit_deferred",
                        "tenant": context.tenant,
                        "session_id": context.session_id,
                        "kind": context.frame.header.kind,
                        "unit_id": context.unit_id,
                    },
                )
                return Outcome.deferred("precondition not met")
            except Exception:
                logger.exception(
                    "unit_failed",
                    extra={
                        "event": "unit_failed",
                        "tenant": context.tenant,
                        "session_id": context.session_id,
                        "kind": context.frame.header.kind,
                        "unit_id": context.unit_id,
                    },
                )
                raise
            return outcome

    async def _redeliver(self, context: UnitContext) -> Outcome:
        """重放已 committed 的分片/清单，重新物化丢失的中间态。

        收据不再改写、响应仍是 duplicates；处理器内部只做幂等副作用（同一
        内容落同一对象/分片）。中间态仍未齐时以 RollbackUnit 收场，等同跳过。
        """
        try:
            async with self.repository.transaction():
                await self._handlers[context.frame.header.kind](context)
        except RollbackUnit:
            logger.info(
                "unit_redeliver_still_incomplete",
                extra={
                    "event": "unit_redeliver_still_incomplete",
                    "tenant": context.tenant,
                    "session_id": context.session_id,
                    "kind": context.frame.header.kind,
                    "unit_id": context.unit_id,
                },
            )
        return Outcome.duplicate()

    async def _refresh_agent_env_envelope(self, context: UnitContext) -> Outcome:
        """`agent.env` 重报：内容未变时只把信封事实收敛到较新的一次。

        配置变化走新 `contentHash`（新单元、新 blob，§4.6）；**分类变化不改变载荷**，
        所以客户端重发同一单元、只换信封的 `workspaceKind`/`capturedAt`——服务端以此
        落地会话行三列（client-design §4.4「同一内容的新分类靠服务端对 envelope 列的
        覆写落地」）。三条边界：

        - 收据已是终态：响应仍是 `duplicates`（客户端推进 ack，不产生新持久对象）；
        - 只有内容仍是当前锚点时才刷新——旧 blob 的重放不得回退锚点或汰换新对象；
        - `capturedAt` 不倒退——崩溃后乱序重放的旧信封不覆盖已落库的较新事实。
        """
        session_id = context.session_id or ""
        payload_sha256 = context.frame.header.payload_sha256
        state = await self.repository.get_agent_config_anchor_state(context.tenant, session_id)
        if state is None or state.agent_config_sha256 != payload_sha256:
            return Outcome.duplicate()
        workspace_kind = context.frame.header.workspace_kind
        captured_at = parse_rfc3339(context.frame.header.captured_at)
        if workspace_kind not in WORKSPACE_KINDS or captured_at is None:
            return Outcome.duplicate()
        stored = state.agent_config_captured_at
        if stored is not None and captured_at <= stored:
            return Outcome.duplicate()
        async with self.repository.transaction():
            await self.repository.set_agent_config_anchor(
                context.tenant,
                session_id,
                payload_sha256,
                workspace_kind=workspace_kind,
                captured_at=captured_at,
            )
        logger.info(
            "agent_env_envelope_refreshed",
            extra={
                "event": "agent_env_envelope_refreshed",
                "tenant": context.tenant,
                "session_id": session_id,
                "workspace_kind": workspace_kind,
                "captured_at": captured_at.isoformat(),
            },
        )
        return Outcome.duplicate()

    # ── 收据与隔离 ──────────────────────────────────────────────────────────

    async def _record_rejection(
        self, principal: Principal, frame: Frame, code: RejectionCode, message: str
    ) -> None:
        await self.repository.put_receipt(
            ReceiptRecord(
                tenant=principal.tenant,
                unit_id=frame.header.unit_id,
                sha256=frame.header.payload_sha256,
                state=RECEIPT_REJECTED,
                code=str(code),
                message=message,
            )
        )

    async def _isolate(
        self,
        principal: Principal,
        frame: Frame,
        state: str,
        code: str,
    ) -> None:
        """未知 kind / 更高帧版本：原样写隔离对象 + 收据 `unsupported`，不解析内容（§3.1-4）。"""
        async with self.repository.transaction():
            if frame.payload:
                await self.objects.put(principal.tenant, frame.header.payload_sha256, frame.payload)
                await self.repository.put_object(
                    ObjectRecord(
                        tenant=principal.tenant,
                        sha256=frame.header.payload_sha256,
                        size_bytes=len(frame.payload),
                        channel=QUARANTINE_CHANNEL,
                        state=OBJECT_VERIFIED,
                    )
                )
            await self.repository.put_receipt(
                ReceiptRecord(
                    tenant=principal.tenant,
                    unit_id=frame.header.unit_id,
                    sha256=frame.header.payload_sha256,
                    state=state,
                    code=code,
                    message=f"kind={frame.header.kind} v={frame.header.v}",
                )
            )

    # ── 处理器：轨迹 ────────────────────────────────────────────────────────

    async def _handle_trajectory_lines(self, context: UnitContext) -> Outcome:
        if not context.session_id:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID, "trajectory.lines requires session_id"
            )
        match = SCOPE_TRAJECTORY.match(context.scope)
        if match is None:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID, "trajectory.lines scope must be lines:<first>-<last>"
            )
        first = int(match.group("first"))
        last = int(match.group("last"))
        if last < first:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID, "trajectory.lines scope has an empty range"
            )
        async with self._session_locks.hold((context.tenant, context.session_id)):
            outcome = await self.trajectory.apply_lines(
                tenant=context.tenant,
                session_id=context.session_id,
                payload=context.payload,
                reset=context.frame.header.reset,
            )
        if outcome.kind is OutcomeKind.COMMITTED:
            self._check_scope_range(context, first, last)
        return outcome

    def _check_scope_range(self, context: UnitContext, first: int, last: int) -> None:
        """scope 的行区间与载荷内容不符时留证（不改变已提交的结果）。"""
        sequences = []
        for line in context.payload.split(b"\n"):
            if not line:
                continue
            try:
                sequences.append(parse_line(line).sequence)
            except ValueError:
                return
        if sequences and (min(sequences) < first or max(sequences) > last):
            logger.warning(
                "trajectory_scope_mismatch",
                extra={
                    "event": "trajectory_scope_mismatch",
                    "tenant": context.tenant,
                    "session_id": context.session_id,
                    "first": first,
                    "last": last,
                },
            )

    # ── 处理器：快照 ────────────────────────────────────────────────────────

    async def _handle_snapshot_chunk(self, context: UnitContext) -> Outcome:
        if not context.session_id:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID, "snapshot.chunk requires session_id"
            )
        match = SCOPE_SNAPSHOT_CHUNK.match(context.scope)
        if match is None:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID,
                "snapshot.chunk scope must be chunk:<content_hash>:<index>",
            )
        content_hash = match.group("sha")
        # 分块本身就是内容寻址对象：落 OBS + 登记（幂等，重放即重新物化）。
        await self.objects.put(context.tenant, context.frame.header.payload_sha256, context.payload)
        await self.repository.put_object(
            ObjectRecord(
                tenant=context.tenant,
                sha256=context.frame.header.payload_sha256,
                size_bytes=len(context.payload),
                channel=SNAPSHOT_CHUNK_CHANNEL,
                state=OBJECT_VERIFIED,
            )
        )
        # 该快照的清单已到（登记为缺块行）→ 缺口一补齐就立刻拼装整份快照。
        pending = await self.repository.get_object(context.tenant, content_hash)
        if (
            pending is not None
            and pending.channel == SNAPSHOT_CHANNEL
            and pending.state == OBJECT_MISSING
            and pending.missing_hashes
        ):
            assembled = await self.assembler.assemble(
                tenant=context.tenant,
                session_id=context.session_id,
                content_hash=content_hash,
                chunk_hashes=pending.missing_hashes,
            )
            if assembled:
                logger.info(
                    "snapshot_assembled",
                    extra={
                        "event": "snapshot_assembled",
                        "tenant": context.tenant,
                        "session_id": context.session_id,
                        "content_hash": content_hash,
                        "trigger": "snapshot.chunk",
                    },
                )
        return Outcome.committed()

    async def _handle_snapshot_manifest(self, context: UnitContext) -> Outcome:
        if not context.session_id:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID, "snapshot.manifest requires session_id"
            )
        match = SCOPE_SNAPSHOT_MANIFEST.match(context.scope)
        if match is None:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID,
                "snapshot.manifest scope must be manifest:<content_hash>",
            )
        content_hash = match.group("sha")
        try:
            document = json.loads(context.payload)
        except (ValueError, UnicodeDecodeError) as exc:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID, f"snapshot.manifest is not valid JSON: {exc}"
            )
        if not isinstance(document, dict):
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID, "snapshot.manifest must be a JSON object"
            )
        if document.get("content_hash") != content_hash:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID,
                "snapshot.manifest content_hash does not match the scope",
            )
        chunks = document.get("chunks")
        if (
            not isinstance(chunks, list)
            or not chunks
            or not all(isinstance(item, str) and len(item) == 64 for item in chunks)
        ):
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID, "snapshot.manifest chunks must be sha256 digests"
            )
        size = document.get("size")
        chunk_size = document.get("chunk_size")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID,
                "snapshot.manifest size must be a non-negative integer",
            )
        if not isinstance(chunk_size, int) or isinstance(chunk_size, bool) or chunk_size <= 0:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID, "snapshot.manifest chunk_size must be positive"
            )
        if size and len(chunks) != (size + chunk_size - 1) // chunk_size:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID,
                "snapshot.manifest chunk count does not match size/chunk_size",
            )
        if size > self.settings.max_snapshot_bytes:
            return Outcome.rejected(
                RejectionCode.PAYLOAD_TOO_LARGE,
                f"snapshot.manifest size {size} bytes exceeds the limit",
            )
        existing = await self.repository.get_object(context.tenant, content_hash)
        if existing is not None and existing.state == OBJECT_VERIFIED:
            return Outcome.committed()
        missing = await self.assembler.missing_chunks(context.tenant, chunks)
        if not missing:
            # 分块对象都在：就地拼装。拼不出来（哈希不符/对象丢失）按载荷错误处理。
            assembled = await self.assembler.assemble(
                tenant=context.tenant,
                session_id=context.session_id,
                content_hash=content_hash,
                chunk_hashes=chunks,
            )
            if not assembled:
                return Outcome.rejected(
                    RejectionCode.PAYLOAD_SHA_MISMATCH,
                    "snapshot chunks do not reproduce the declared content_hash",
                )
            logger.info(
                "snapshot_assembled",
                extra={
                    "event": "snapshot_assembled",
                    "tenant": context.tenant,
                    "session_id": context.session_id,
                    "content_hash": content_hash,
                    "trigger": "snapshot.manifest",
                },
            )
            return Outcome.committed()
        # 清单是持久事实：登记缺块行（missing_hashes = 期望分块清单），
        # 后续任意分片到达或协商端点都能据此继续拼装（跨重启成立）。
        await self.repository.put_object(
            ObjectRecord(
                tenant=context.tenant,
                sha256=content_hash,
                size_bytes=size,
                channel=SNAPSHOT_CHANNEL,
                state=OBJECT_MISSING,
                missing_hashes=tuple(chunks),
            )
        )
        return Outcome.committed()

    async def _store_assembled(
        self, context: UnitContext, *, sha256: str, data: bytes, channel: str
    ) -> Outcome:
        actual = hashlib.sha256(data).hexdigest()
        if actual != sha256:
            return Outcome.rejected(
                RejectionCode.PAYLOAD_SHA_MISMATCH,
                "assembled bytes do not match the target sha256",
            )
        await self.objects.put(context.tenant, sha256, data)
        await self.repository.put_object(
            ObjectRecord(
                tenant=context.tenant,
                sha256=sha256,
                size_bytes=len(data),
                channel=channel,
                state=OBJECT_VERIFIED,
            )
        )
        return Outcome.committed()

    # ── 处理器：工作区 ──────────────────────────────────────────────────────

    async def _handle_workspace_file(self, context: UnitContext) -> Outcome:
        if not context.session_id:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID, "workspace.file requires session_id"
            )
        match = SCOPE_WORKSPACE_FILE.match(context.scope)
        if match is None:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID, "workspace.file scope must be wsfile:<sha256>:<index>"
            )
        target = match.group("sha")
        index = int(match.group("index"))
        result = await self.assemblies.add_slice(
            tenant=context.tenant,
            session_id=context.session_id,
            kind=WORKSPACE_FILE,
            target=target,
            index=index,
            data=context.payload,
            chunk_sha256=context.frame.header.payload_sha256,
        )
        if result.over_budget:
            # 超预算是背压不是淘汰：无副作用，客户端按退避重发（§3.2-4）。
            return Outcome.deferred("staging budget exceeded; retry later")
        if result.complete and result.data is not None:
            return await self._store_assembled(
                context, sha256=target, data=result.data, channel="workspace-file"
            )
        return Outcome.committed()

    async def _handle_workspace_manifest(self, context: UnitContext) -> Outcome:
        if not context.session_id:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID, "workspace.manifest requires session_id"
            )
        match = SCOPE_WORKSPACE_MANIFEST.match(context.scope)
        if match is None:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID,
                "workspace.manifest scope must be ws:<manifest_sha256>",
            )
        try:
            manifest = parse_manifest(context.payload)
            refs = content_refs(manifest)
            identity = repository_identity(manifest)
            # 忽略清单校验（含单条 path ≤ 512）与结构校验同口径 → schema_invalid。
            ignored_count, ignored_truncated, ignored_entries = ignored_summary(manifest)
        except WorkspaceManifestError as exc:
            return Outcome.rejected(RejectionCode.SCHEMA_INVALID, str(exc))

        verified = await self.repository.verified_objects(
            context.tenant, [ref.sha256 for ref in refs]
        )
        missing = [ref.sha256 for ref in refs if ref.sha256 not in verified]
        if missing:
            # 任一条目缺块 → 该 manifest 返回 deferred（无副作用，客户端重发）。
            return Outcome.deferred(f"{len(missing)} workspace files are not verified yet")
        try:
            contents = [await self.objects.get(context.tenant, ref.sha256) for ref in refs]
            container = build_container(manifest, contents)
        except WorkspaceManifestError as exc:
            return Outcome.rejected(RejectionCode.SCHEMA_INVALID, str(exc))
        except Exception as exc:  # 对象缺失/读取失败：引用无法满足
            return Outcome.rejected(RejectionCode.MISSING_REFERENCE, str(exc))

        body_sha = container_sha256(container)
        await self.objects.put(context.tenant, body_sha, container)
        await self.repository.put_object(
            ObjectRecord(
                tenant=context.tenant,
                sha256=body_sha,
                size_bytes=len(container),
                channel="workspace-state",
                state=OBJECT_VERIFIED,
            )
        )
        counts = summary_counts(manifest)
        head = head_fields(manifest)
        capture = manifest.get("capture")
        capture_map = capture if isinstance(capture, dict) else {}
        session_block = manifest.get("session")
        session_map = session_block if isinstance(session_block, dict) else {}
        await self.repository.upsert_workspace_state(
            WorkspaceStateRecord(
                tenant=context.tenant,
                session_id=context.session_id,
                repository_identity=identity,
                user_id=context.user_id,
                remote_kind=remote_kind(manifest),
                body_sha256=body_sha,
                body_size_bytes=len(container),
                head_sha=head["head_sha"],
                head_tree_sha=head["head_tree_sha"],
                branch=head["branch"],
                detached=head["detached"],
                staged_entries=counts["staged"],
                worktree_entries=counts["worktree"],
                untracked_entries=counts["untracked"],
                omitted_count=counts["omitted"],
                complete=bool(capture_map.get("complete", True)),
                session_started_at=parse_rfc3339(session_map.get("startedAt")),
                captured_at=parse_rfc3339(capture_map.get("completedAt")),
                capture_duration_ms=_int_or_none(capture_map.get("durationMs")),
                ignored_count=ignored_count,
                ignored_truncated=ignored_truncated,
                ignored_entries=ignored_entries,
            )
        )
        return Outcome.committed()

    # ── 处理器：会话、环境、仓库 ────────────────────────────────────────────

    async def _handle_session_meta(self, context: UnitContext) -> Outcome:
        if not context.session_id:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID, "session.meta requires session_id"
            )
        match = SCOPE_SESSION_META.match(context.scope)
        if match is None:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID, "session.meta scope must be meta:<content_hash>"
            )
        content_hash = match.group("sha")
        try:
            meta = parse_session_meta(context.payload)
        except SessionMetaError as exc:
            return Outcome.rejected(RejectionCode.SCHEMA_INVALID, str(exc))

        snapshot = await self.repository.get_object(context.tenant, content_hash)
        if snapshot is None or snapshot.state != OBJECT_VERIFIED:
            return Outcome.deferred("snapshot content_hash is not verified yet")

        # 环境三列（agent_config_sha256 / workspace_kind / agent_config_captured_at）
        # 只由 agent.env 写（agent-config-sync §1）：session.meta 不解析、不校验、
        # 不清除；行建立时由仓储从锚点 coalesce。
        observed_at = datetime.now(UTC)
        record = SessionRecord(
            tenant=context.tenant,
            session_id=context.session_id,
            user_id=context.user_id,
            content_hash=content_hash,
            observed_at=observed_at,
            updated_at=meta.updated_at or observed_at,
            created_at=meta.created_at,
            title=meta.title,
            agent_profile=meta.agent_profile,
            model_provider=meta.model_provider,
            model_id=meta.model_id,
            producer_version=meta.app_version,
            parent_session_id=meta.parent_session_id,
            primary_cwd_digest=meta.primary_cwd_digest,
            turn_count=meta.turn_count,
            message_count=meta.message_count,
            total_tokens=meta.total_tokens,
            repository_identities=meta.repository_identities,
            snapshot_sha256=content_hash,
            snapshot_size=snapshot.size_bytes,
            snapshot_state="ok",
            linked="linked",
        )
        advanced = await self.repository.upsert_session(record)
        if not advanced:
            logger.info(
                "stale_report",
                extra={
                    "event": "stale_report",
                    "tenant": context.tenant,
                    "session_id": context.session_id,
                    "unit_id": context.unit_id,
                },
            )
        else:
            logger.info(
                "session_linked",
                extra={
                    "event": "session_linked",
                    "tenant": context.tenant,
                    "session_id": context.session_id,
                    "user_id": context.user_id,
                    "snapshot_sha256": content_hash,
                    "link": "linked",
                },
            )
        return Outcome.committed()

    async def _handle_agent_env(self, context: UnitContext) -> Outcome:
        if not context.session_id:
            return Outcome.rejected(RejectionCode.SCHEMA_INVALID, "agent.env requires session_id")
        match = SCOPE_AGENT_ENV.match(context.scope)
        if match is None:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID, "agent.env scope must be agentenv:<sha256>"
            )
        sha256 = match.group("sha")
        if sha256 != context.frame.header.payload_sha256:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID,
                "agent.env scope sha256 must equal sha256(payload)",
            )
        # 信封（agent-config-sync §1）：workspaceKind 三值枚举 + capturedAt 必填 RFC3339。
        workspace_kind = context.frame.header.workspace_kind
        if workspace_kind not in WORKSPACE_KINDS:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID,
                f"agent.env envelope workspaceKind must be one of {sorted(WORKSPACE_KINDS)}",
            )
        captured_at = parse_rfc3339(context.frame.header.captured_at)
        if captured_at is None:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID, "agent.env envelope capturedAt must be RFC3339"
            )
        try:
            parse_agent_config(context.payload, max_bytes=self.settings.agent_env_max_bytes)
        except AgentConfigError as exc:
            return Outcome.rejected(RejectionCode.SCHEMA_INVALID, str(exc))
        await self.objects.put(context.tenant, sha256, context.payload)
        await self.repository.put_object(
            ObjectRecord(
                tenant=context.tenant,
                sha256=sha256,
                size_bytes=len(context.payload),
                channel="agent-config",
                state=OBJECT_VERIFIED,
            )
        )
        # 覆写汰换（agent-config-sync §1）：同会话换新配置时，旧引用对象置
        # superseded（OBS 字节保留），三列随锚点推进。
        previous = await self.repository.get_agent_config_anchor(context.tenant, context.session_id)
        if previous is not None and previous != sha256:
            await self.repository.mark_object_superseded(context.tenant, previous)
        await self.repository.set_agent_config_anchor(
            context.tenant,
            context.session_id,
            sha256,
            workspace_kind=workspace_kind,
            captured_at=captured_at,
        )
        return Outcome.committed()

    async def _handle_repo_identity(self, context: UnitContext) -> Outcome:
        match = SCOPE_REPOSITORY.match(context.scope)
        if match is None:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID, "repo.identity scope must be repo:<identity>"
            )
        try:
            identity = parse_repository_identity(context.payload)
        except ValueError as exc:
            return Outcome.rejected(RejectionCode.SCHEMA_INVALID, str(exc))
        if match.group("identity") != identity.identity:
            return Outcome.rejected(
                RejectionCode.SCHEMA_INVALID,
                "repo.identity scope must equal repositoryIdentity",
            )
        observed_at = parse_rfc3339(identity.observed_at) or datetime.now(UTC)
        await self.repository.upsert_repository(
            RepositoryRecord(
                tenant=context.tenant,
                local_identity=identity.identity,
                repository_key=identity.repository_key,
                provider=identity.provider,
                connection_id="report",
                host=identity.host,
                api_path=identity.path,
                resolution="reported",
                user_id=context.user_id,
                observed_at=observed_at,
            )
        )
        return Outcome.committed()

    # ── 定时清扫：缺块终态 ──────────────────────────────────────────────────

    async def _maybe_sweep(self) -> None:
        now = time.monotonic()
        if now - self._last_sweep < self.settings.assembly_sweep_interval_s:
            return
        self._last_sweep = now
        for missing in self.assemblies.expired():
            async with self.repository.transaction():
                await self.repository.put_object(
                    ObjectRecord(
                        tenant=missing.tenant,
                        sha256=missing.target,
                        size_bytes=0,
                        channel="workspace-file",
                        state=OBJECT_MISSING,
                        missing_hashes=missing.missing_hashes,
                    )
                )
                if missing.session_id:
                    await self.repository.set_session_body_state(
                        missing.tenant, missing.session_id, "missing"
                    )
            logger.warning(
                "assembly_missing",
                extra={
                    "event": "assembly_missing",
                    "tenant": missing.tenant,
                    "session_id": missing.session_id,
                    "kind": missing.kind,
                    "target": missing.target,
                    "missing": len(missing.missing_hashes),
                },
            )
        reclaimed = await asyncio.to_thread(self.assemblies.cleanup_expired)
        if reclaimed:
            logger.info(
                "staging_orphans_reclaimed",
                extra={"event": "staging_orphans_reclaimed", "directories": reclaimed},
            )


def _int_or_none(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value
