"""字节块暂存与组装（server-acceptance §3.2）。

`snapshot.chunk`（`chunk:<content_hash>:<index>`）与 `workspace.file`
（`wsfile:<sha256>:<index>`）共用同一套机制：按目标 sha 聚合分片、按分片序拼接、
`sha256(拼接结果) == 目标 sha` 才算组装完成。

分片落在本地暂存目录（内存只留元数据），受「单会话 / 单租户字节预算 + TTL」约束：
超预算按最旧未完成组装淘汰（`assembly_evicted`），超 TTL 置缺块终态（`missing`）。
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

SNAPSHOT = "snapshot"
WORKSPACE_FILE = "wsfile"


@dataclass(frozen=True, slots=True)
class AssemblyResult:
    complete: bool
    data: bytes | None = None
    missing_indexes: tuple[int, ...] = ()


@dataclass(slots=True)
class Assembly:
    tenant: str
    session_id: str
    kind: str
    target: str
    created_at: float
    updated_at: float
    chunks: dict[int, tuple[int, str]] = field(default_factory=dict)  # index -> (size, sha)
    # TTL 只置缺块终态，**不丢弃**已到的分片：缺口治愈后状态可单向推进为 verified（§3.2-5）。
    marked_missing: bool = False

    @property
    def total_bytes(self) -> int:
        return sum(size for size, _sha in self.chunks.values())

    def missing_indexes(self) -> tuple[int, ...]:
        if not self.chunks:
            return ()
        highest = max(self.chunks)
        return tuple(index for index in range(highest + 1) if index not in self.chunks)


@dataclass(frozen=True, slots=True)
class MissingAssembly:
    tenant: str
    session_id: str
    kind: str
    target: str
    missing_hashes: tuple[str, ...]


class AssemblyStore:
    def __init__(
        self,
        root: Path | str,
        *,
        per_session_bytes: int = 64 * 1024 * 1024,
        per_tenant_bytes: int = 2 * 1024 * 1024 * 1024,
        ttl_s: float = 24 * 3600.0,
        clock=time.time,
    ) -> None:
        self._root = Path(root)
        self._per_session_bytes = per_session_bytes
        self._per_tenant_bytes = per_tenant_bytes
        self._ttl_s = ttl_s
        self._clock = clock
        self._assemblies: dict[tuple[str, str, str], Assembly] = {}
        self._expected_chunks: dict[tuple[str, str], tuple[str, ...]] = {}
        self._staged_hashes: dict[tuple[str, str], set[str]] = {}
        self._lock = asyncio.Lock()

    # ── 分片写入 ────────────────────────────────────────────────────────────

    async def add_slice(
        self,
        *,
        tenant: str,
        session_id: str,
        kind: str,
        target: str,
        index: int,
        data: bytes,
        chunk_sha256: str,
    ) -> AssemblyResult:
        async with self._lock:
            key = (tenant, f"{kind}:{target}")
            assembly = self._assemblies.get(key)
            now = self._clock()
            if assembly is None:
                assembly = Assembly(
                    tenant=tenant,
                    session_id=session_id,
                    kind=kind,
                    target=target,
                    created_at=now,
                    updated_at=now,
                )
                self._assemblies[key] = assembly
            if index in assembly.chunks and assembly.chunks[index][1] == chunk_sha256:
                return AssemblyResult(complete=False, missing_indexes=assembly.missing_indexes())
            await self._write_chunk(tenant, kind, target, index, data)
            assembly.chunks[index] = (len(data), chunk_sha256)
            assembly.session_id = session_id
            assembly.updated_at = now
            self._staged_hashes.setdefault((tenant, target), set()).add(chunk_sha256)
            self._evict_if_needed(tenant)
            return await self._try_assemble(assembly)

    async def _write_chunk(
        self, tenant: str, kind: str, target: str, index: int, data: bytes
    ) -> None:
        path = self._chunk_path(tenant, kind, target, index)
        await asyncio.to_thread(self._write_chunk_sync, path, data)

    @staticmethod
    def _write_chunk_sync(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_bytes(data)
        temporary.replace(path)

    async def _try_assemble(self, assembly: Assembly) -> AssemblyResult:
        missing = assembly.missing_indexes()
        if missing:
            return AssemblyResult(complete=False, missing_indexes=missing)
        if not assembly.chunks:
            return AssemblyResult(complete=False)
        parts: list[bytes] = []
        for index in sorted(assembly.chunks):
            path = self._chunk_path(assembly.tenant, assembly.kind, assembly.target, index)
            try:
                parts.append(await asyncio.to_thread(path.read_bytes))
            except FileNotFoundError:
                return AssemblyResult(complete=False, missing_indexes=(index,))
        data = b"".join(parts)
        if hashlib.sha256(data).hexdigest() != assembly.target:
            return AssemblyResult(complete=False)
        self._discard(assembly.tenant, assembly.kind, assembly.target)
        self._assemblies.pop((assembly.tenant, f"{assembly.kind}:{assembly.target}"), None)
        return AssemblyResult(complete=True, data=data)

    async def assemble_existing(self, tenant: str, kind: str, target: str) -> AssemblyResult:
        assembly = self._assemblies.get((tenant, f"{kind}:{target}"))
        if assembly is None:
            return AssemblyResult(complete=False)
        async with self._lock:
            return await self._try_assemble(assembly)

    # ── 预算与淘汰 ──────────────────────────────────────────────────────────

    def _evict_if_needed(self, tenant: str) -> None:
        mine = [item for item in self._assemblies.values() if item.tenant == tenant]
        session_bytes: dict[str, int] = {}
        for item in mine:
            session_bytes[item.session_id] = (
                session_bytes.get(item.session_id, 0) + item.total_bytes
            )
        for session_id, total in list(session_bytes.items()):
            if total <= self._per_session_bytes:
                continue
            candidates = sorted(
                (item for item in mine if item.session_id == session_id),
                key=lambda item: item.updated_at,
            )
            for item in candidates:
                if session_bytes[session_id] <= self._per_session_bytes:
                    break
                logger.warning(
                    "assembly_evicted",
                    extra={
                        "event": "assembly_evicted",
                        "tenant": tenant,
                        "session_id": item.session_id,
                        "kind": item.kind,
                        "target": item.target,
                    },
                )
                session_bytes[session_id] -= item.total_bytes
                self._drop(item)
        total_bytes = sum(
            item.total_bytes for item in self._assemblies.values() if item.tenant == tenant
        )
        if total_bytes <= self._per_tenant_bytes:
            return
        for item in sorted(mine, key=lambda item: item.updated_at):
            if total_bytes <= self._per_tenant_bytes:
                break
            logger.warning(
                "assembly_evicted",
                extra={
                    "event": "assembly_evicted",
                    "scope": "tenant",
                    "tenant": tenant,
                    "session_id": item.session_id,
                    "kind": item.kind,
                    "target": item.target,
                },
            )
            total_bytes -= item.total_bytes
            self._drop(item)

    def _drop(self, assembly: Assembly) -> None:
        self._assemblies.pop((assembly.tenant, f"{assembly.kind}:{assembly.target}"), None)
        self._discard(assembly.tenant, assembly.kind, assembly.target)

    def _discard(self, tenant: str, kind: str, target: str) -> None:
        shutil.rmtree(self._root / tenant / kind / target, ignore_errors=True)

    def _chunk_path(self, tenant: str, kind: str, target: str, index: int) -> Path:
        return self._root / tenant / kind / target / f"{index:06d}.bin"

    # ── 缺块协商所需的旁路信息 ────────────────────────────────────────────────

    def record_expected_chunks(
        self, tenant: str, content_hash: str, chunk_hashes: tuple[str, ...]
    ) -> None:
        self._expected_chunks[(tenant, content_hash)] = chunk_hashes

    def expected_chunks(self, tenant: str, content_hash: str) -> tuple[str, ...] | None:
        return self._expected_chunks.get((tenant, content_hash))

    def staged_hashes(self, tenant: str, target: str) -> set[str]:
        return set(self._staged_hashes.get((tenant, target), set()))

    def peek(self, tenant: str, kind: str, target: str) -> Assembly | None:
        return self._assemblies.get((tenant, f"{kind}:{target}"))

    # ── TTL 与缺块终态 ──────────────────────────────────────────────────────

    def expired(self) -> list[MissingAssembly]:
        now = self._clock()
        expired: list[MissingAssembly] = []
        for assembly in list(self._assemblies.values()):
            if assembly.marked_missing:
                continue
            if now - assembly.updated_at <= self._ttl_s:
                continue
            missing = assembly.missing_indexes()
            hashes = tuple(f"index:{index}" for index in missing) or tuple(sorted(assembly.chunks))
            assembly.marked_missing = True
            expired.append(
                MissingAssembly(
                    tenant=assembly.tenant,
                    session_id=assembly.session_id,
                    kind=assembly.kind,
                    target=assembly.target,
                    missing_hashes=hashes,
                )
            )
        return expired
