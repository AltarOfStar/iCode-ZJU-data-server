"""`agent.env` 载荷校验（`agent-config/1.0`，server-acceptance §3.2-7）。

正文只进 OBS（内容寻址），不进 PG；`sha256(payload)` 即对象键。
校验：JSON 可解析、`redacted[]` 存在、`schema` 匹配；超长按 `schema_invalid` 拒绝（不截断）。
"""

from __future__ import annotations

import json

AGENT_CONFIG_SCHEMA = "agent-config/1.0"


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
    schema = document.get("schema")
    if schema is not None and schema != AGENT_CONFIG_SCHEMA:
        raise AgentConfigError(f"unsupported agent-config schema: {schema!r}")
    redacted = document.get("redacted")
    if not isinstance(redacted, list):
        raise AgentConfigError("agent.env payload must carry redacted[]")
    if not all(isinstance(item, str) for item in redacted):
        raise AgentConfigError("agent.env redacted[] must contain key names only")
    return document
