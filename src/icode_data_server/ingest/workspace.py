"""IGWS 工作区容器拼装（server-acceptance §3.2-6，纯函数）。

容器格式与 teams `src/client/workspace-state.ts` 逐字节一致（小端）：

```
"IGWS" | version:u16 | flags:u16 | manifestLength:u32 | manifest JSON | 内容流
```

- `version = 1`；`flags` bit0 = `complete`（无任何省略）；
- manifest 条目按 manifest 顺序拼接内容流，`content.offset` 相对内容流起点；
- 删除条目与 gitlink 的 `content` 为 `null`；
- 忽略清单（`ignored`）只在上报的索引载荷里，**不进容器**（07 §1，容器字节格式不变）。

同一 manifest + 同一内容集合 → 同一字节，因此拼装是确定性纯函数。
"""

from __future__ import annotations

import hashlib
import json
import struct
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

WORKSPACE_STATE_SCHEMA = "workspace-state/1.0"
MAGIC = b"IGWS"
VERSION = 1
HEADER_BYTES = 12
WORKSPACE_IGNORED_MAX = 200

_IGNORED_KEY = "ignored"


class WorkspaceManifestError(ValueError):
    """manifest 结构性错误 → `schema_invalid`。"""


@dataclass(frozen=True, slots=True)
class ContentRef:
    offset: int
    length: int
    sha256: str


def parse_manifest(payload: bytes) -> dict[str, Any]:
    try:
        document = json.loads(payload)
    except (ValueError, UnicodeDecodeError) as exc:
        raise WorkspaceManifestError(f"workspace manifest is not valid JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise WorkspaceManifestError("workspace manifest must be a JSON object")
    schema = document.get("schema")
    if schema != WORKSPACE_STATE_SCHEMA:
        raise WorkspaceManifestError(f"unsupported workspace manifest schema: {schema!r}")
    for section in ("staged", "worktree"):
        block = document.get(section)
        if not isinstance(block, dict) or not isinstance(block.get("entries", []), list):
            raise WorkspaceManifestError(f"workspace manifest {section}.entries is missing")
    untracked = document.get("untracked")
    if not isinstance(untracked, dict) or not isinstance(untracked.get("entries", []), list):
        raise WorkspaceManifestError("workspace manifest untracked.entries is missing")
    if not isinstance(document.get("omitted", []), list):
        raise WorkspaceManifestError("workspace manifest omitted must be a list")
    return document


def iter_entries(manifest: Mapping[str, Any]):
    """按 manifest 顺序产出 (section, entry)；内容流顺序 = 该顺序（offset 递增）。"""
    for section in ("staged", "worktree", "untracked"):
        block = manifest.get(section)
        if not isinstance(block, Mapping):
            continue
        entries = block.get("entries")
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if isinstance(entry, Mapping):
                yield section, entry


def content_ref(entry: Mapping[str, Any]) -> ContentRef | None:
    raw = entry.get("content")
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise WorkspaceManifestError("entry content must be an object or null")
    offset = raw.get("offset")
    length = raw.get("length")
    sha256 = raw.get("sha256")
    if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
        raise WorkspaceManifestError("entry content.offset must be a non-negative integer")
    if not isinstance(length, int) or isinstance(length, bool) or length < 0:
        raise WorkspaceManifestError("entry content.length must be a non-negative integer")
    if not isinstance(sha256, str) or len(sha256) != 64:
        raise WorkspaceManifestError("entry content.sha256 must be a sha256 hex digest")
    return ContentRef(offset=offset, length=length, sha256=sha256)


def content_refs(manifest: Mapping[str, Any]) -> tuple[ContentRef, ...]:
    """所有条目内容引用，按内容流顺序返回（要求 offset 连续覆盖整个内容流）。"""
    refs: list[ContentRef] = []
    for _section, entry in iter_entries(manifest):
        ref = content_ref(entry)
        if ref is not None:
            refs.append(ref)
    ordered = sorted(refs, key=lambda ref: ref.offset)
    cursor = 0
    for ref in ordered:
        if ref.offset != cursor:
            raise WorkspaceManifestError("content offsets must be contiguous from 0")
        cursor += ref.length
    return tuple(ordered)


def container_manifest_bytes(manifest: Mapping[str, Any]) -> bytes:
    """容器内的 manifest JSON：丢弃只属于索引载荷的 `ignored`，其余保持原始键序。"""
    trimmed = {key: value for key, value in manifest.items() if key != _IGNORED_KEY}
    return json.dumps(trimmed, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def build_container(manifest: Mapping[str, Any], contents: Sequence[bytes]) -> bytes:
    """按 teams 的容器格式拼装；`contents` 必须与 `content_refs(manifest)` 一一对应。"""
    refs = content_refs(manifest)
    if len(refs) != len(contents):
        raise WorkspaceManifestError("content references and contents length mismatch")
    for ref, data in zip(refs, contents, strict=True):
        if len(data) != ref.length:
            raise WorkspaceManifestError("content length does not match manifest")
        if hashlib.sha256(data).hexdigest() != ref.sha256:
            raise WorkspaceManifestError("content sha256 does not match manifest")
    manifest_bytes = container_manifest_bytes(manifest)
    capture = manifest.get("capture")
    complete = bool(capture.get("complete", True)) if isinstance(capture, Mapping) else True
    header = MAGIC + struct.pack("<HHI", VERSION, 1 if complete else 0, len(manifest_bytes))
    return b"".join([header, manifest_bytes, *contents])


def container_sha256(container: bytes) -> str:
    return hashlib.sha256(container).hexdigest()


def summary_counts(manifest: Mapping[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {"staged": 0, "worktree": 0, "untracked": 0, "omitted": 0}
    for section, _entry in iter_entries(manifest):
        counts[section] = counts.get(section, 0) + 1
    omitted = manifest.get("omitted")
    counts["omitted"] = len(omitted) if isinstance(omitted, list) else 0
    return counts


def ignored_summary(manifest: Mapping[str, Any]) -> tuple[int, bool, tuple[str, ...]]:
    """忽略清单只保留路径（目录折叠、≤200 条、超出 `truncated`），正文永不入库。"""
    block = manifest.get(_IGNORED_KEY)
    if not isinstance(block, Mapping):
        return 0, False, ()
    entries = block.get("entries")
    paths: list[str] = []
    if isinstance(entries, list):
        for entry in entries[:WORKSPACE_IGNORED_MAX]:
            if isinstance(entry, str):
                paths.append(entry)
            elif isinstance(entry, Mapping):
                path = entry.get("path")
                if isinstance(path, str):
                    paths.append(path)
    truncated = bool(block.get("truncated", False)) or (
        isinstance(entries, list) and len(entries) > WORKSPACE_IGNORED_MAX
    )
    count = block.get("count")
    if not isinstance(count, int) or isinstance(count, bool):
        count = len(entries) if isinstance(entries, list) else len(paths)
    return count, truncated, tuple(paths)


def repository_identity(manifest: Mapping[str, Any]) -> str:
    repository = manifest.get("repository")
    identity = repository.get("identity") if isinstance(repository, Mapping) else None
    if not isinstance(identity, str) or not identity:
        raise WorkspaceManifestError("workspace manifest repository.identity is missing")
    return identity


def remote_kind(manifest: Mapping[str, Any]) -> str:
    repository = manifest.get("repository")
    remote = repository.get("remote") if isinstance(repository, Mapping) else None
    kind = remote.get("kind") if isinstance(remote, Mapping) else None
    return kind if isinstance(kind, str) and kind else "unknown"


def head_fields(manifest: Mapping[str, Any]) -> dict[str, Any]:
    repository = manifest.get("repository")
    head = repository.get("head") if isinstance(repository, Mapping) else None
    if not isinstance(head, Mapping):
        return {"head_sha": None, "head_tree_sha": None, "branch": None, "detached": False}
    return {
        "head_sha": _str_or_none(head.get("sha")),
        "head_tree_sha": _str_or_none(head.get("treeSha")),
        "branch": _str_or_none(head.get("branch")),
        "detached": bool(head.get("detached", False)),
    }


def _str_or_none(value: object) -> str | None:
    return value if isinstance(value, str) and value else None
