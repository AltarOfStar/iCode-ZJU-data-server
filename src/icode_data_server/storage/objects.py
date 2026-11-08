"""对象存储抽象（server-acceptance §4.1）。

键布局 `t/<tenant>/o/<sha256[0:2]>/<sha256>`，对象不可变、内容去重。
`S3ObjectStore` 通过 `endpoint_url` 对接 OBS；`LocalObjectStore` 是同键布局的文件系统实现，
用于开发与测试。
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import threading
from pathlib import Path
from typing import Protocol

from .credentials import EcsMetadataCredentials


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
    """boto3 实现；`endpoint_url` 指向 OBS 端点，boto3 的同步调用全部放到线程里。

    华为云 OBS 有三处与 AWS S3 的差异（均已实测）：
    - 委托身份要带 `security token`，且约 24 小时过期 → `credentials` 负责取与刷新；
    - botocore ≥1.36 默认加 CRC32 + `aws-chunked` 尾校验，OBS 会报
      `XAmzContentSHA256Mismatch` → 显式改成"仅必要时计算"；
    - 没有 ListBucket 权限时，不存在的键返回 403 而不是 404 → 读写路径统一按"缺失"处理。
    """

    def __init__(
        self,
        *,
        bucket: str,
        endpoint_url: str = "",
        region: str = "",
        access_key_id: str = "",
        secret_access_key: str = "",
        session_token: str = "",
        credentials: EcsMetadataCredentials | None = None,
        prefix: str = "",
    ) -> None:
        if not bucket:
            raise ValueError("S3ObjectStore requires a bucket")
        self._bucket = bucket
        self._endpoint_url = endpoint_url or None
        self._region = region or None
        self._access_key_id = access_key_id or None
        self._secret_access_key = secret_access_key or None
        self._session_token = session_token or None
        self._credentials = credentials
        self._prefix = prefix.strip("/")
        self._client = None
        self._client_lock = threading.Lock()

    def _key(self, tenant: str, sha256: str) -> str:
        key = object_key(tenant, sha256)
        return f"{self._prefix}/{key}" if self._prefix else key

    def _current_credentials(self) -> tuple[str | None, str | None, str | None]:
        if self._credentials is not None:
            access, secret, token = self._credentials.resolve()
            return access, secret, token or None
        return self._access_key_id, self._secret_access_key, self._session_token

    def _ensure_client(self):
        if self._client is not None and not self._credentials_stale():
            return self._client
        with self._client_lock:
            if self._client is not None and not self._credentials_stale():
                return self._client
            import boto3
            from botocore.config import Config

            access, secret, token = self._current_credentials()
            self._client = boto3.client(
                "s3",
                endpoint_url=self._endpoint_url,
                region_name=self._region,
                aws_access_key_id=access,
                aws_secret_access_key=secret,
                aws_session_token=token,
                config=Config(
                    request_checksum_calculation="when_required",
                    response_checksum_validation="when_required",
                ),
            )
            return self._client

    def _credentials_stale(self) -> bool:
        return self._credentials is not None and self._credentials.stale()

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
            try:
                response = self._ensure_client().get_object(
                    Bucket=self._bucket, Key=self._key(tenant, sha256)
                )
            except Exception as exc:  # botocore 的 ClientError 只在运行时可得
                if _is_not_found(exc):
                    # 与 LocalObjectStore 保持一致：缺对象抛 FileNotFoundError，
                    # 上层（snapshots / assembly）靠它把"对象丢了"和"后端坏了"分开。
                    raise FileNotFoundError(self._key(tenant, sha256)) from exc
                raise
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
    """`404 / NoSuchKey` 是标准语义；OBS 在缺少 ListBucket 权限时对缺失键返回 403。

    实测 `head_object` 缺失键的 `Error.Code` 是 `"403"`，`get_object` 是 `"AccessDenied"`，
    所以 403 两种码都要认（拿不到 ListBucket 时它无法区分"不存在"和"没权限"）。
    """
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    error = response.get("Error", {})
    code = str(error.get("Code")) if isinstance(error, dict) else ""
    if code in {"404", "NoSuchKey", "NotFound"}:
        return True
    meta = response.get("ResponseMetadata", {})
    status = meta.get("HTTPStatusCode") if isinstance(meta, dict) else None
    if status == 404:
        return True
    return status == 403 and code in {"403", "AccessDenied", ""}
