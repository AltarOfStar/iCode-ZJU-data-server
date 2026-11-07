"""`session.meta` 载荷校验（`session/1.0` 白名单，server-acceptance §3.4.4）。

服务端不解析 `session.json`：这些值由客户端从进程内已有值构造后上报。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

SESSION_SCHEMA = "session/1.0"


class SessionMetaError(ValueError):
    """载荷不符合 `session/1.0` 白名单 → `schema_invalid`。"""


@dataclass(frozen=True, slots=True)
class SessionMeta:
    title: str | None = None
    agent_profile: str | None = None
    model_provider: str | None = None
    model_id: str | None = None
    app_version: str | None = None
    parent_session_id: str | None = None
    primary_cwd_digest: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    schema_version: int | None = None
    turn_count: int = 0
    message_count: int = 0
    total_tokens: int = 0
    repository_identities: tuple[str, ...] = ()


def parse_session_meta(payload: bytes) -> SessionMeta:
    try:
        document = json.loads(payload)
    except (ValueError, UnicodeDecodeError) as exc:
        raise SessionMetaError(f"session.meta payload is not valid JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise SessionMetaError("session.meta payload must be a JSON object")
    schema = document.get("schemaVersion")
    if schema is not None and not isinstance(schema, int):
        raise SessionMetaError("session.meta schemaVersion must be an integer")
    if "environment" in document:
        # `session/1.0` 不携带环境引用（agent-config-sync §1）：三列只由 agent.env 写。
        # 按未知字段显式拒绝，不做兼容读取。
        raise SessionMetaError(
            "session.meta no longer carries environment;"
            " agent-config facts arrive on agent.env frames"
        )
    identities = document.get("repositoryIdentities")
    if identities is None:
        repository_identities: tuple[str, ...] = ()
    elif isinstance(identities, list) and all(isinstance(item, str) for item in identities):
        repository_identities = tuple(dict.fromkeys(identities))[:16]
    else:
        raise SessionMetaError("session.meta repositoryIdentities must be a list of strings")
    return SessionMeta(
        title=_text(document.get("title")),
        agent_profile=_text(document.get("agentProfile")),
        model_provider=_text(document.get("modelProvider")),
        model_id=_text(document.get("modelId")),
        app_version=_text(document.get("appVersion")),
        parent_session_id=_text(document.get("parentSessionId")),
        primary_cwd_digest=_text(document.get("primaryCwdDigest")),
        created_at=_timestamp(document.get("createdAt")),
        updated_at=_timestamp(document.get("updatedAt")),
        schema_version=schema,
        turn_count=_count(document.get("turnCount")),
        message_count=_count(document.get("messageCount")),
        total_tokens=_count(document.get("totalTokens")),
        repository_identities=repository_identities,
    )


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _count(value: object) -> int:
    parsed = _count_or_none(value)
    return 0 if parsed is None else parsed


def _count_or_none(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def parse_rfc3339(value: str | None) -> datetime | None:
    """RFC3339 → aware UTC datetime；不做任何猜测式补全。"""
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z") or text.endswith("z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _timestamp(value: object) -> datetime | None:
    return parse_rfc3339(value) if isinstance(value, str) else None
