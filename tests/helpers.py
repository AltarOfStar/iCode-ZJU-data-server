"""测试用的帧与单元构造器（合成帧驱动，不依赖客户端实现）。"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from icode_data_server.frames import encode_frame, payload_sha256, unit_id_for

SESSION = "11111111-1111-4111-8111-111111111111"
DEV_TOKEN = "dev-token"


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def unit(
    kind: str,
    scope: str,
    payload: bytes,
    *,
    session_id: str = SESSION,
    reset: bool = False,
    v: int = 1,
    unit_id: str | None = None,
    payload_sha: str | None = None,
) -> bytes:
    digest = payload_sha if payload_sha is not None else payload_sha256(payload)
    header = {
        "v": v,
        "kind": kind,
        "session_id": session_id,
        "scope": scope,
        "payload_sha256": digest,
        "payload_len": len(payload),
        "created_at": "2026-11-06T00:00:00Z",
        "reset": reset,
        "unit_id": unit_id if unit_id is not None else unit_id_for(kind, session_id, scope, digest),
    }
    return encode_frame(header, payload)


def batch(*frames: bytes) -> bytes:
    return b"".join(frames)


def unit_id(kind: str, scope: str, payload: bytes, session_id: str = SESSION) -> str:
    return unit_id_for(kind, session_id, scope, payload_sha256(payload))


# ── 快照 ────────────────────────────────────────────────────────────────────


def snapshot_bytes(size: int = 700 * 1024) -> bytes:
    alphabet = "iCode-ZJU 快照 payload ✓ ".encode()
    data = bytearray()
    while len(data) < size:
        data.extend(alphabet)
    return bytes(data[:size])


def chunk_payloads(content: bytes, chunk_size: int = 256 * 1024) -> list[bytes]:
    return [content[i : i + chunk_size] for i in range(0, len(content), chunk_size)]


def snapshot_manifest_payload(content: bytes, chunk_size: int = 256 * 1024) -> bytes:
    chunks = [sha(chunk) for chunk in chunk_payloads(content, chunk_size)]
    return json.dumps(
        {
            "content_hash": sha(content),
            "size": len(content),
            "chunk_size": chunk_size,
            "chunks": chunks,
        },
        separators=(",", ":"),
    ).encode()


def snapshot_frame(index: int, chunk: bytes, content_hash: str, session_id: str = SESSION) -> bytes:
    return unit(
        "snapshot.chunk",
        f"chunk:{content_hash}:{index}",
        chunk,
        session_id=session_id,
    )


# ── session.meta ────────────────────────────────────────────────────────────


def session_meta_payload(
    content_hash: str,
    *,
    updated_at: str = "2026-11-06T10:00:00Z",
    workspace_kind: str | None = "git",
    agent_config_sha: str | None = None,
    title: str = "s",
    repository_identities: list[str] | None = None,
) -> bytes:
    environment: dict[str, Any] = {}
    if workspace_kind is not None:
        environment["workspaceKind"] = workspace_kind
    if agent_config_sha is not None:
        environment["agentConfigSha256"] = agent_config_sha
        environment["agentConfigSize"] = 123
    document = {
        "schemaVersion": 1,
        "title": title,
        "agentProfile": "default",
        "modelProvider": "anthropic",
        "modelId": "claude",
        "appVersion": "1.2.3",
        "primaryCwdDigest": "a" * 32,
        "createdAt": "2026-11-06T09:00:00Z",
        "updatedAt": updated_at,
        "turnCount": 2,
        "messageCount": 9,
        "totalTokens": 1234,
        "repositoryIdentities": repository_identities or [],
        "environment": environment,
    }
    return json.dumps(document, ensure_ascii=False, separators=(",", ":")).encode()


# ── 工作区（IGWS） ───────────────────────────────────────────────────────────


def workspace_manifest(
    *,
    files: list[tuple[str, bytes]],
    identity: str = "repo-identity-1",
    complete: bool = True,
    ignored: list[str] | None = None,
    session_id: str = SESSION,
) -> tuple[bytes, list[tuple[str, bytes]]]:
    """构造 workspace-state/1.0 manifest，返回 (manifest JSON, [(sha, bytes)])。

    内容流按 staged → worktree → untracked 的顺序追加，
    条目内容引用 `content{offset,length,sha256}`。
    """
    staged_entries: list[dict[str, Any]] = []
    contents: list[tuple[str, bytes]] = []
    offset = 0
    for path, data in files:
        digest = sha(data)
        contents.append((digest, data))
        staged_entries.append(
            {
                "path": path,
                "status": "M",
                "mode": "100644",
                "content": {"offset": offset, "length": len(data), "sha256": digest},
            }
        )
        offset += len(data)
    manifest: dict[str, Any] = {
        "schema": "workspace-state/1.0",
        "createdAt": "2026-11-06T10:00:00Z",
        "session": {"sessionId": session_id, "cwd": "/repo"},
        "capture": {
            "startedAt": "2026-11-06T09:59:59Z",
            "completedAt": "2026-11-06T10:00:00Z",
            "durationMs": 12,
            "complete": complete,
            "budgetMs": 10000,
        },
        "repository": {
            "root": "/repo",
            "commonDir": "/repo/.git",
            "isWorktree": False,
            "identity": identity,
            "head": {
                "sha": "f" * 40,
                "treeSha": "e" * 40,
                "branch": "main",
                "detached": False,
            },
            "remote": {"kind": "origin", "url": "https://github.com/o/r.git"},
        },
        "staged": {"base": "head", "entries": staged_entries},
        "worktree": {"base": "index", "entries": []},
        "untracked": {"entries": []},
        "omitted": [],
    }
    if ignored is not None:
        manifest["ignored"] = {
            "count": len(ignored),
            "truncated": False,
            "entries": ignored,
        }
    payload = json.dumps(manifest, ensure_ascii=False, separators=(",", ":")).encode()
    return payload, contents


def workspace_file_frames(
    contents: list[tuple[str, bytes]], *, session_id: str = SESSION, chunk_size: int = 1024 * 1024
) -> list[bytes]:
    frames: list[bytes] = []
    for digest, data in contents:
        slices = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)] or [b""]
        for index, slice_bytes in enumerate(slices):
            frames.append(
                unit(
                    "workspace.file",
                    f"wsfile:{digest}:{index}",
                    slice_bytes,
                    session_id=session_id,
                )
            )
    return frames


# ── agent.env / repo.identity ───────────────────────────────────────────────


def agent_env_payload(*, schema: str = "agent-config/1.0", redacted: Any = None) -> bytes:
    document = {
        "schema": schema,
        "appVersion": "1.2.3",
        "platform": "win32-x64",
        "profile": {"name": "default", "yaml": "model: claude\n"},
        "model": {"profileId": "p1", "provider": "anthropic", "modelId": "claude", "params": {}},
        "tools": {"enabled": ["read_file"], "kinds": {}},
        "skills": [],
        "mcp": {"servers": []},
        "hooks": {"count": 0, "events": []},
        "settingsDigest": "b" * 64,
        "redacted": ["api_key", "headers"] if redacted is None else redacted,
    }
    return json.dumps(document, ensure_ascii=False, separators=(",", ":")).encode()


def repository_payload(
    *, identity: str = "repo-identity-1", host: str = "github.com", path: str = "o/r"
) -> bytes:
    document = {
        "schema": "repository/1.0",
        "repositoryIdentity": identity,
        "remote": {"host": host, "path": path, "urlKind": "https"},
        "remoteDigest": "c" * 64,
        "observedAt": "2026-11-06T10:00:00Z",
    }
    return json.dumps(document, ensure_ascii=False, separators=(",", ":")).encode()


def trajectory_payload(lines: list[bytes]) -> bytes:
    return b"".join(line + b"\n" for line in lines)


async def send_snapshot(
    client,
    content: bytes,
    *,
    session_id: str = SESSION,
    chunk_size: int = 256 * 1024,
    order: list[int] | None = None,
    token: str | None = DEV_TOKEN,
) -> tuple[str, list[bytes]]:
    """把一份快照按块（可乱序）连同 manifest 一起上报，返回 (content_hash, chunks)。"""
    content_hash = sha(content)
    chunks = chunk_payloads(content, chunk_size)
    indices = list(range(len(chunks))) if order is None else order
    frames = [snapshot_frame(index, chunks[index], content_hash, session_id) for index in indices]
    frames.append(
        unit(
            "snapshot.manifest",
            f"manifest:{content_hash}",
            snapshot_manifest_payload(content, chunk_size),
            session_id=session_id,
        )
    )
    await client.post_batch(batch(*frames), token=token)
    return content_hash, chunks
