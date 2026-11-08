"""对象存储抽象（server-acceptance §4.1）。

键布局 `t/<tenant>/o/<sha256[0:2]>/<sha256>`，对象不可变、内容去重。
`S3ObjectStore` 通过 `endpoint_url` 对接 OBS；`LocalObjectStore` 是同键布局的文件系统实现，
用于开发与测试。
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path
from typing import Protocol


def object_key(tenant: str, sha256: str) -> str:
    return f"t/{tenant}/o/{sha256[:2]}/{sha256}"


class ObjectStore(Protocol):
    async def start(self) -> None: ...

    async def close(self) -> None: ...

    async def put(self, tenant: str, sha256: str, data: bytes) -> None: ...

    async def get(self, tenant: str, sha256: str) -> bytes: ...

    async def exists(self, tenant: str, sha256: str) -> bool: ...


class LocalObjectStore:
    """文件系统对象存储：原子写入（临时文件 + rename），键布局与 OBS 相同。"""

    def __init__(self, root: Path | str) -> None:
        self._root = Path(root)

    @property
    def root(self) -> Path:
        return self._root

    def path_for(self, tenant: str, sha256: str) -> Path:
        return self._root / object_key(tenant, sha256)

    async def start(self) -> None:
        await asyncio.to_thread(lambda: self._root.mkdir(parents=True, exist_ok=True))

    async def close(self) -> None:
        return None

    async def put(self, tenant: str, sha256: str, data: bytes) -> None:
        await asyncio.to_thread(self._put_sync, tenant, sha256, data)

    def _put_sync(self, tenant: str, sha256: str, data: bytes) -> None:
        target = self.path_for(tenant, sha256)
        if target.exists() and target.stat().st_size == len(data):
            return
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, raw = tempfile.mkstemp(prefix=".tmp-", dir=target.parent)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(raw, target)
        except BaseException:
            Path(raw).unlink(missing_ok=True)
            raise

    async def get(self, tenant: str, sha256: str) -> bytes:
        return await asyncio.to_thread(self.path_for(tenant, sha256).read_bytes)

    async def exists(self, tenant: str, sha256: str) -> bool:
        return await asyncio.to_thread(self.path_for(tenant, sha256).is_file)


class S3ObjectStore:
    """boto3 实现；`endpoint_url` 指向 OBS 端点，boto3 的同步调用全部放到线程里。"""

    def __init__(
        self,
        *,
        bucket: str,
        endpoint_url: str = "",
        region: str = "",
        access_key_id: str = "",
        secret_access_key: str = "",
        prefix: str = "",
    ) -> None:
        if not bucket:
            raise ValueError("S3ObjectStore requires a bucket")
        self._bucket = bucket
        self._endpoint_url = endpoint_url or None
        self._region = region or None
        self._access_key_id = access_key_id or None
        self._secret_access_key = secret_access_key or None
        self._prefix = prefix.strip("/")
        self._client = None

    def _key(self, tenant: str, sha256: str) -> str:
        key = object_key(tenant, sha256)
        return f"{self._prefix}/{key}" if self._prefix else key

    def _ensure_client(self):
        if self._client is None:
            import boto3

            self._client = boto3.client(
                "s3",
                endpoint_url=self._endpoint_url,
                region_name=self._region,
                aws_access_key_id=self._access_key_id,
                aws_secret_access_key=self._secret_access_key,
            )
        return self._client

    async def start(self) -> None:
        await asyncio.to_thread(self._ensure_client)

    async def close(self) -> None:
        self._client = None

    async def put(self, tenant: str, sha256: str, data: bytes) -> None:
        def _put() -> None:
            self._ensure_client().put_object(
                Bucket=self._bucket,
                Key=self._key(tenant, sha256),
                Body=data,
                ContentType="application/octet-stream",
                Metadata={"sha256": sha256},
            )

        await asyncio.to_thread(_put)

    async def get(self, tenant: str, sha256: str) -> bytes:
        def _get() -> bytes:
            response = self._ensure_client().get_object(
                Bucket=self._bucket, Key=self._key(tenant, sha256)
            )
            return response["Body"].read()

        return await asyncio.to_thread(_get)

    async def exists(self, tenant: str, sha256: str) -> bool:
        def _head() -> bool:
            client = self._ensure_client()
            try:
                client.head_object(Bucket=self._bucket, Key=self._key(tenant, sha256))
            except Exception as exc:  # botocore 的 ClientError 只在运行时可得
                if _is_not_found(exc):
                    return False
                raise
            return True

        return await asyncio.to_thread(_head)


def _is_not_found(exc: BaseException) -> bool:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        error = response.get("Error", {})
        if isinstance(error, dict) and str(error.get("Code")) in {"404", "NoSuchKey", "NotFound"}:
            return True
        meta = response.get("ResponseMetadata", {})
        if isinstance(meta, dict) and meta.get("HTTPStatusCode") == 404:
            return True
    return False
