"""自定义帧编解码（client-design §4.1）。

```
frame := u32BE(header_len) | header_json(UTF-8) | payload(payload_len bytes)
```

``unit_id`` 是 ``sha256(kind | session_id | scope | payload_sha256)`` 的 64 位十六进制——
只从内容派生，不掺入进程/运行实例标识，因此重开、换机、重装后自然衔接。
"""

from __future__ import annotations

import hashlib
import json
import struct
from dataclasses import dataclass
from typing import Any

FRAME_CONTENT_TYPE = "application/vnd.chrys.report.frames"
FRAME_VERSION = 1

_U32 = struct.Struct(">I")


class FrameError(ValueError):
    """帧流结构性错误：整批无法继续解析，按 400 终态处理。"""


def unit_id_for(kind: str, session_id: str, scope: str, payload_sha256: str) -> str:
    # 分隔符取规格字面量 `kind | session_id | scope | payload_sha256` 的 `|`。
    material = "|".join((kind, session_id, scope, payload_sha256))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def payload_sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class FrameHeader:
    v: int
    kind: str
    unit_id: str
    session_id: str
    scope: str
    payload_sha256: str
    payload_len: int
    created_at: str | None = None
    reset: bool = False


@dataclass(frozen=True, slots=True)
class Frame:
    header: FrameHeader
    payload: bytes

    @property
    def unit_id(self) -> str:
        return self.header.unit_id


def _require_str(raw: dict[str, Any], key: str, *, required: bool = True) -> str:
    value = raw.get(key)
    if value is None:
        if required:
            raise FrameError(f"frame header missing {key!r}")
        return ""
    if not isinstance(value, str):
        raise FrameError(f"frame header {key!r} must be a string")
    return value


def header_from_json(raw: Any) -> FrameHeader:
    if not isinstance(raw, dict):
        raise FrameError("frame header must be a JSON object")
    version = raw.get("v", FRAME_VERSION)
    if not isinstance(version, int) or isinstance(version, bool):
        raise FrameError("frame header 'v' must be an integer")
    payload_len = raw.get("payload_len")
    if not isinstance(payload_len, int) or isinstance(payload_len, bool) or payload_len < 0:
        raise FrameError("frame header 'payload_len' must be a non-negative integer")
    reset = raw.get("reset", False)
    if not isinstance(reset, bool):
        raise FrameError("frame header 'reset' must be a boolean")
    return FrameHeader(
        v=version,
        kind=_require_str(raw, "kind"),
        unit_id=_require_str(raw, "unit_id"),
        session_id=_require_str(raw, "session_id", required=False),
        scope=_require_str(raw, "scope"),
        payload_sha256=_require_str(raw, "payload_sha256"),
        payload_len=payload_len,
        created_at=raw.get("created_at") if isinstance(raw.get("created_at"), str) else None,
        reset=reset,
    )


def decode_frames(body: bytes) -> list[Frame]:
    """把顺序拼接的帧流拆成帧列表；输入必须是完整帧流（WAL 尾部不完整帧由调用方截断）。"""
    frames: list[Frame] = []
    offset = 0
    total = len(body)
    while offset < total:
        if total - offset < _U32.size:
            raise FrameError("truncated frame length prefix")
        (header_len,) = _U32.unpack_from(body, offset)
        offset += _U32.size
        if header_len == 0 or total - offset < header_len:
            raise FrameError("truncated frame header")
        raw_header = body[offset : offset + header_len]
        offset += header_len
        try:
            parsed = json.loads(raw_header)
        except (ValueError, UnicodeDecodeError) as exc:
            raise FrameError(f"frame header is not valid JSON: {exc}") from exc
        header = header_from_json(parsed)
        if total - offset < header.payload_len:
            raise FrameError("truncated frame payload")
        payload = body[offset : offset + header.payload_len]
        offset += header.payload_len
        frames.append(Frame(header=header, payload=payload))
    return frames


def encode_frame(header: dict[str, Any], payload: bytes) -> bytes:
    """测试与客户端工具用：按契约编码单帧。"""
    header = dict(header)
    header.setdefault("v", FRAME_VERSION)
    header.setdefault("payload_len", len(payload))
    header.setdefault("payload_sha256", payload_sha256(payload))
    header.setdefault(
        "unit_id",
        unit_id_for(
            str(header.get("kind", "")),
            str(header.get("session_id", "")),
            str(header.get("scope", "")),
            str(header["payload_sha256"]),
        ),
    )
    raw = json.dumps(header, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return _U32.pack(len(raw)) + raw + payload


def encode_frames(frames: list[tuple[dict[str, Any], bytes]]) -> bytes:
    return b"".join(encode_frame(header, payload) for header, payload in frames)
