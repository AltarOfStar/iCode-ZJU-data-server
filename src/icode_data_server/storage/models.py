"""存储层领域模型：与 teams v10 在范围内表的列一一对应（server-acceptance §4.2）。"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime

# ingest_receipts.state 取值（§3.1-2）：pending 行在同一事务内先写，
# 副作用完成置 committed，终态拒绝置 rejected，未知类型置 unsupported。
RECEIPT_PENDING = "pending"
RECEIPT_COMMITTED = "committed"
RECEIPT_REJECTED = "rejected"
RECEIPT_UNSUPPORTED = "unsupported"
TERMINAL_RECEIPT_STATES = frozenset({RECEIPT_COMMITTED, RECEIPT_REJECTED, RECEIPT_UNSUPPORTED})

# object_registry.state 取值（schema.ts v4 + 本服务 §3.2-5 的缺块终态）
OBJECT_VERIFIED = "verified"
OBJECT_PENDING = "pending"
OBJECT_MISSING = "missing"
OBJECT_SUPERSEDED = "superseded"


@dataclass(frozen=True, slots=True)
class ReceiptRecord:
    tenant: str
    unit_id: str
    sha256: str
    state: str
    channel: str = "report"
    code: str | None = None
    message: str | None = None
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ObjectRecord:
    tenant: str
    sha256: str
    size_bytes: int
    channel: str
    state: str = OBJECT_VERIFIED
    etag: str | None = None
    missing_hashes: tuple[str, ...] = ()
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class SessionRecord:
    """`sessions` 行（PK = tenant + external_session_id）。"""

    tenant: str
    session_id: str
    user_id: str
    content_hash: str
    observed_at: datetime
    updated_at: datetime | None = None
    created_at: datetime | None = None
    title: str | None = None
    agent_profile: str | None = None
    model_provider: str | None = None
    model_id: str | None = None
    producer_version: str | None = None
    parent_session_id: str | None = None
    primary_cwd_digest: str | None = None
    turn_count: int = 0
    message_count: int = 0
    total_tokens: int = 0
    repository_identities: tuple[str, ...] = ()
    snapshot_sha256: str | None = None
    snapshot_size: int | None = None
    snapshot_state: str = "ok"
    body_state: str | None = None
    # v10 环境复现三列：唯一写入者是 agent.env（agent-config-sync §1）
    agent_config_sha256: str | None = None
    workspace_kind: str | None = None
    agent_config_captured_at: datetime | None = None
    linked: str = "unlinked"

    def with_updates(self, **changes: object) -> SessionRecord:
        return replace(self, **changes)


@dataclass(frozen=True, slots=True)
class TurnRecord:
    """`session_turns` 行；行键 (session_id, generation, turn_number)。"""

    tenant: str
    session_id: str
    generation: int
    turn_number: int
    turn_id: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    duration_ms: int | None = None
    end_reason: str | None = None


@dataclass(frozen=True, slots=True)
class EventRecord:
    """`trajectory_events` 行；行键 (session_id, generation, sequence)。"""

    tenant: str
    session_id: str
    generation: int
    sequence: int
    batch_id: str
    event_id: str | None
    event_type: str
    turn_id: str | None = None
    operation_id: str | None = None
    actor_kind: str | None = None
    actor_role: str | None = None
    occurred_at: datetime | None = None
    monotonic_ns: int | None = None
    tool_name: str | None = None
    tool_kind: str | None = None
    duration_ms: int | None = None
    outcome: str | None = None
    output_tokens: int | None = None
    input_tokens: int | None = None


@dataclass(frozen=True, slots=True)
class BatchRecord:
    """`event_batches` 行：一个窗口 = 一个批次。"""

    tenant: str
    session_id: str
    batch_id: str
    generation: int
    client_sequence: int
    first_event_sequence: int
    last_event_sequence: int
    event_count: int
    unparsed_count: int
    gap_count: int
    completeness_state: str
    payload_sha256: str
    payload_size: int


@dataclass(frozen=True, slots=True)
class WorkspaceStateRecord:
    """`workspace_states` 行：一个会话 × 一个仓库一行，正文是服务端拼装的 IGWS 容器。"""

    tenant: str
    session_id: str
    repository_identity: str
    user_id: str
    remote_kind: str
    body_sha256: str
    body_size_bytes: int
    head_sha: str | None = None
    head_tree_sha: str | None = None
    branch: str | None = None
    detached: bool = False
    staged_entries: int = 0
    worktree_entries: int = 0
    untracked_entries: int = 0
    omitted_count: int = 0
    complete: bool = True
    session_started_at: datetime | None = None
    captured_at: datetime | None = None
    capture_duration_ms: int | None = None
    ignored_count: int = 0
    ignored_truncated: bool = False
    ignored_entries: tuple[str, ...] = ()
    body_etag: str | None = None


@dataclass(frozen=True, slots=True)
class RepositoryRecord:
    """`repository_registry` 行（PK = tenant + local_identity）。"""

    tenant: str
    local_identity: str
    repository_key: str
    provider: str
    connection_id: str
    host: str
    api_path: str
    resolution: str
    user_id: str
    observed_at: datetime
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class SessionAnchor:
    """`session_anchors`：服务端本地簿记（非 teams 表）。

    `agent.env` 可能在 `session.meta` 之前到达，锚点先落这里，等 `sessions` 行建立时 coalesce。
    """

    tenant: str
    session_id: str
    agent_config_sha256: str | None = None
    workspace_kind: str | None = None
    agent_config_captured_at: datetime | None = None
    updated_at: datetime | None = None
