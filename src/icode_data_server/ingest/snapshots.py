"""快照组装（server-acceptance §3.2-3）：分块是内容寻址对象，清单到齐即可拼装。

`snapshot.chunk` 的载荷本身就是一块内容（≤ 256 KiB），直接按 `payload_sha256`
存入对象库并登记 `object_registry`；`snapshot.manifest` 携带有序分块清单，
任何时刻（包括进程重启之后）只要全部分块对象已验证，就能重新拼出整份快照。
增量上报因此天然成立：客户端只发变化块，未变化的块沿用已登记的对象。

拼装结果的 `sha256` 必须等于清单声明的 `content_hash`，否则拒绝登记。
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Sequence

from ..storage.models import OBJECT_VERIFIED, ObjectRecord
from ..storage.objects import ObjectStore
from ..storage.repository import Repository

logger = logging.getLogger(__name__)

SNAPSHOT_CHANNEL = "session-snapshot"
SNAPSHOT_CHUNK_CHANNEL = "snapshot-chunk"


class SnapshotAssembler:
    """manifest 驱动的快照拼装，`BatchProcessor` 与缺块协商端点共用。"""

    def __init__(self, repository: Repository, objects: ObjectStore) -> None:
        self.repository = repository
        self.objects = objects

    async def missing_chunks(self, tenant: str, chunk_hashes: Sequence[str]) -> list[str]:
        verified = await self.repository.verified_objects(tenant, chunk_hashes)
        return [sha for sha in chunk_hashes if sha not in verified]

    async def assemble(
        self, *, tenant: str, session_id: str, content_hash: str, chunk_hashes: Sequence[str]
    ) -> bool:
        """尝试用已登记的分块对象拼出 `content_hash`；成功则登记整份快照对象。

        幂等：重复调用只会再次得到同一对象。失败（缺块 / 对象丢失 / 哈希不符）
        不留任何登记，返回 False。
        """
        if not chunk_hashes:
            return False
        missing = await self.missing_chunks(tenant, chunk_hashes)
        if missing:
            return False
        parts: list[bytes] = []
        try:
            for sha in chunk_hashes:
                parts.append(await self.objects.get(tenant, sha))
        except FileNotFoundError:
            logger.warning(
                "snapshot_chunk_object_lost",
                extra={
                    "event": "snapshot_chunk_object_lost",
                    "tenant": tenant,
                    "session_id": session_id,
                    "content_hash": content_hash,
                },
            )
            return False
        data = b"".join(parts)
        if hashlib.sha256(data).hexdigest() != content_hash:
            logger.warning(
                "snapshot_hash_mismatch",
                extra={
                    "event": "snapshot_hash_mismatch",
                    "tenant": tenant,
                    "session_id": session_id,
                    "content_hash": content_hash,
                    "chunks": len(chunk_hashes),
                },
            )
            return False
        etag = await self.objects.put(tenant, content_hash, data)
        await self.repository.put_object(
            ObjectRecord(
                tenant=tenant,
                sha256=content_hash,
                size_bytes=len(data),
                channel=SNAPSHOT_CHANNEL,
                state=OBJECT_VERIFIED,
                etag=etag,
            )
        )
        return True
