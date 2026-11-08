"""字节块暂存与组装（server-acceptance §3.2）。

`workspace.file`（`wsfile:<sha256>:<index>`）按目标 sha 聚合分片、按分片序拼接、
`sha256(拼接结果) == 目标 sha` 才算组装完成。快照分块不再走这里：它们本身是
内容寻址对象，由 `SnapshotAssembler` 按清单拼装（见 `snapshots.py`）。

分片落在本地暂存目录（内存只留元数据），受「单会话 / 单租户字节预算 + TTL」
约束：

- 超预算 **不淘汰**：该分片返回 `deferred`（无副作用），客户端按退避重发——
  淘汰已收下的分片等于静默丢数据，收据又已 committed，客户端永远不会补发。
- 超 TTL 置缺块终态（`missing`）并释放分片字节；客户端通过缺块协商得知后
  重发，重放走 `duplicates` 的重新物化路径（§2.2），缺口治愈后状态单向推进
  为 verified（§3.2-5）。
- 重启后内存索引丢失，但收据仍在：客户端重发的分片以 `duplicates` 重新落盘，
  组装照常完成；孤儿目录（客户端已消失）由 `cleanup_expired` 按 mtime 回收。
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

WORKSPACE_FILE = "wsfile"


@dataclass(frozen=True, slots=True)
class AssemblyResult:
    complete: bool
    data: bytes | None = None
    missing_indexes: tuple[int, ...] = ()
    over_budget: bool = False


@dataclass(slots=True)
class Assembly:
    tenant: str
    session_id: str
    kind: str
    target: str
    created_at: float
    updated_at: float
    chunks: dict[int, tuple[int, str]] = field(default_factory=dict)  # index -> (size, sha)

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
        self._assemblies: dict[tuple[str, str], Assembly] = {}
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
            if self._over_budget(tenant, session_id, len(data)):
                return AssemblyResult(complete=False, over_budget=True)
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
                return await self._try_assemble(assembly)
            await self._write_chunk(tenant, kind, target, index, data)
            assembly.chunks[index] = (len(data), chunk_sha256)
            assembly.session_id = session_id
            assembly.updated_at = now
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

    # ── 预算：超限即背压，不淘汰 ────────────────────────────────────────────

    def _over_budget(self, tenant: str, session_id: str, extra_bytes: int) -> bool:
        session_total = 0
        tenant_total = 0
        for item in self._assemblies.values():
            if item.tenant != tenant:
                continue
            tenant_total += item.total_bytes
            if item.session_id == session_id:
                session_total += item.total_bytes
        if session_total + extra_bytes > self._per_session_bytes:
            return True
        return tenant_total + extra_bytes > self._per_tenant_bytes

    def _discard(self, tenant: str, kind: str, target: str) -> None:
        shutil.rmtree(self._root / tenant / kind / target, ignore_errors=True)

    def _chunk_path(self, tenant: str, kind: str, target: str, index: int) -> Path:
        return self._root / tenant / kind / target / f"{index:06d}.bin"

    # ── TTL 与缺块终态 ──────────────────────────────────────────────────────

    def expired(self) -> list[MissingAssembly]:
        """超时未完成的组装：置缺块终态并释放分片字节。

        缺口治愈不依赖这些分片——客户端重发会重新落盘（`duplicates` 重新物化），
        所以释放字节是安全的，也避免暂存盘只涨不降。
        """
        now = self._clock()
        expired: list[MissingAssembly] = []
        for key, assembly in list(self._assemblies.items()):
            if now - assembly.updated_at <= self._ttl_s:
                continue
            missing = assembly.missing_indexes()
            hashes = tuple(f"index:{index}" for index in missing) or tuple(
                sorted(sha for _size, sha in assembly.chunks.values())
            )
            expired.append(
                MissingAssembly(
                    tenant=assembly.tenant,
                    session_id=assembly.session_id,
                    kind=assembly.kind,
                    target=assembly.target,
                    missing_hashes=hashes,
                )
            )
            self._assemblies.pop(key, None)
            self._discard(assembly.tenant, assembly.kind, assembly.target)
        return expired

    def cleanup_expired(self) -> int:
        """回收孤儿目录：索引已随重启丢失、再无人推进的暂存分片。

        以目录内最新 mtime 判定闲置；活跃组装的写入会持续刷新 mtime，不会被误删。
        返回回收的目录数。
        """
        if not self._root.exists():
            return 0
        deadline = self._clock() - self._ttl_s
        removed = 0
        for target_dir in self._root.glob("*/*/*"):
            if not target_dir.is_dir():
                continue
            try:
                newest = max(entry.stat().st_mtime for entry in target_dir.iterdir())
            except (FileNotFoundError, ValueError):
                newest = target_dir.stat().st_mtime
            if newest >= deadline:
                continue
            shutil.rmtree(target_dir, ignore_errors=True)
            removed += 1
        return removed
