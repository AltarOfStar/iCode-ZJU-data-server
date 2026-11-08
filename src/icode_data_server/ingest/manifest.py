"""缺块协商（`POST /v1/session/{session_id}/manifest`，server-acceptance §2.3）。

`acked_last_sequence` 是**已确认提交**的最大 sequence（不是"收到过"的最大值），
用于客户端校正本地偏移；未知会话按"什么都不存在"处理，不泄漏存在性。

本端点除回答缺口外还有一项修复职责：快照分块是内容寻址对象，若客户端上报的
分块已全部登记而整份快照尚未拼出（典型场景：进程重启丢了内存态、清单侧组装
被打断），这里就地补装——协商一次即治愈，客户端无需重发任何帧。
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from ..storage.repository import Repository
from .snapshots import SnapshotAssembler
from .trajectory import TrajectoryStore


class ManifestRequestError(ValueError):
    """请求体不符合 §2.3 的形状。"""


@dataclass(frozen=True, slots=True)
class ManifestRequest:
    content_hash: str | None
    chunk_hashes: tuple[str, ...]
    manifest_sha256: str | None
    file_shas: tuple[str, ...]


def parse_manifest_request(body: bytes) -> ManifestRequest:
    try:
        document = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ManifestRequestError(f"manifest request is not valid JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise ManifestRequestError("manifest request must be a JSON object")
    snapshot = document.get("snapshot") or {}
    workspace = document.get("workspace") or {}
    if not isinstance(snapshot, dict) or not isinstance(workspace, dict):
        raise ManifestRequestError("snapshot/workspace must be JSON objects")
    content_hash = _hash_or_none(snapshot.get("content_hash"))
    manifest_sha = _hash_or_none(workspace.get("manifest_sha256"))
    return ManifestRequest(
        content_hash=content_hash,
        chunk_hashes=_hash_list(snapshot.get("chunk_hashes"), "snapshot.chunk_hashes"),
        manifest_sha256=manifest_sha,
        file_shas=_hash_list(workspace.get("file_shas"), "workspace.file_shas"),
    )


def _hash_or_none(value: object) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str) or len(value) != 64:
        raise ManifestRequestError("sha256 values must be 64 hex characters")
    return value


def _hash_list(value: object, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ManifestRequestError(f"{field} must be a list")
    hashes: list[str] = []
    for item in value:
        if not isinstance(item, str) or len(item) != 64:
            raise ManifestRequestError(f"{field} must contain sha256 hex digests")
        hashes.append(item)
    return tuple(hashes)


class ManifestService:
    def __init__(
        self,
        repository: Repository,
        assembler: SnapshotAssembler,
        trajectory: TrajectoryStore,
    ) -> None:
        self.repository = repository
        self.assembler = assembler
        self.trajectory = trajectory

    async def reconcile(self, *, tenant: str, session_id: str, request: ManifestRequest) -> dict:
        known_snapshot = False
        missing_chunks: list[str] = []
        if request.content_hash is not None:
            record = await self.repository.get_object(tenant, request.content_hash)
            known_snapshot = record is not None and record.state == "verified"
            if not known_snapshot and request.chunk_hashes:
                missing_chunks = await self.assembler.missing_chunks(tenant, request.chunk_hashes)
                if not missing_chunks:
                    # 分块对象全部在场而快照未拼出：就地补装（重启后的修复路径）。
                    known_snapshot = await self.assembler.assemble(
                        tenant=tenant,
                        session_id=session_id,
                        content_hash=request.content_hash,
                        chunk_hashes=request.chunk_hashes,
                    )

        verified_files = await self.repository.verified_objects(tenant, request.file_shas)
        missing_files = [sha for sha in request.file_shas if sha not in verified_files]

        committed = await self.repository.max_committed_sequence(tenant, session_id)
        acked = max(committed, self.trajectory.applied_sequence(tenant, session_id))

        return {
            "missing_chunk_hashes": missing_chunks,
            "missing_file_shas": missing_files,
            "acked_last_sequence": acked,
            "known_snapshot": known_snapshot,
        }
