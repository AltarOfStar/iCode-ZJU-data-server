"""`agent.env` 载荷与信封校验（`agent-config/1.0`，agent-config-sync §1 / 权威规范 §4.6）。

正文只进 OBS（内容寻址），不进 PG；`sha256(payload)` 即对象键。
校验（与 teams `checkedAgentConfigPut` 同口径）：
- 载荷：可解析为 JSON 对象、`schema` 匹配、`redacted[]` 键名、`sections`（五键、
  `captured|absent`）与 `truncated`（布尔）必填；未知成员宽松忽略；超限显式失败不截断。
- 信封（帧头）：`workspaceKind` 三值枚举、`capturedAt` 必填 RFC3339——见 processor。
"""

from __future__ import annotations

import json

AGENT_CONFIG_SCHEMA = "agent-config/1.0"
WORKSPACE_KINDS = frozenset({"git", "non_git", "no_repo"})

_SECTION_KEYS = frozenset({"profile", "model", "skills", "mcp", "hooks"})
_SECTION_STATES = frozenset({"captured", "absent"})


class AgentConfigError(ValueError):
    """载荷不符合 `agent-config/1.0` → `schema_invalid`。"""


def parse_agent_config(payload: bytes, *, max_bytes: int) -> dict:
    if len(payload) > max_bytes:
        raise AgentConfigError(f"agent.env payload is {len(payload)} bytes, limit is {max_bytes}")
    try:
        document = json.loads(payload)
    except (ValueError, UnicodeDecodeError) as exc:
        raise AgentConfigError(f"agent.env payload is not valid JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise AgentConfigError("agent.env payload must be a JSON object")
    if document.get("schema") != AGENT_CONFIG_SCHEMA:
        raise AgentConfigError(f"unsupported agent-config schema: {document.get('schema')!r}")
    redacted = document.get("redacted")
    if not isinstance(redacted, list):
        raise AgentConfigError("agent.env payload must carry redacted[]")
    if not all(isinstance(item, str) for item in redacted):
        raise AgentConfigError("agent.env redacted[] must contain key names only")
    sections = document.get("sections")
    if not isinstance(sections, dict) or set(sections) != _SECTION_KEYS:
        raise AgentConfigError(
            "agent.env payload must carry sections{profile,model,skills,mcp,hooks}"
        )
    for key, value in sections.items():
        if value not in _SECTION_STATES:
            raise AgentConfigError(
                f"agent.env sections.{key} must be 'captured' or 'absent', got {value!r}"
            )
    if not isinstance(document.get("truncated"), bool):
        raise AgentConfigError("agent.env payload must carry truncated (boolean)")
    return document
