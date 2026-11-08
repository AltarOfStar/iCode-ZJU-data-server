"""华为云 ECS 委托临时凭证（`ecs-agency` + 实例元数据服务）。

`S3ObjectStore` 只接受静态 AK/SK；用委托时凭证由元数据服务签发（约 24 小时过期，
且必须带 security token 一起签名），所以这里负责"取 + 缓存 + 过期前刷新"，不落盘、不打日志。

参考：`~/.local/share/icode-obs/README.md`（同一台机上的 OBS 接入口径）。
"""

from __future__ import annotations

import json
import threading
import time
import urllib.request
from datetime import UTC, datetime

from ..config import S3_METADATA_URL_DEFAULT


class MetadataCredentialError(RuntimeError):
    """元数据服务不可用，或该实例没有绑定 ECS 委托。"""


def _parse_expiry(value: object) -> float | None:
    """`securitykey` 里的 `expires_at` 是 ISO-8601 UTC（如 `2026-11-09T07:12:04.315000Z`）。"""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


class EcsMetadataCredentials:
    """把 `securitykey` 返回的临时 AK/SK 缓存起来，并在过期前重取。"""

    def __init__(
        self,
        url: str = S3_METADATA_URL_DEFAULT,
        *,
        refresh_margin_s: float = 300.0,
        timeout_s: float = 5.0,
    ) -> None:
        self._url = url
        self._refresh_margin_s = refresh_margin_s
        self._timeout_s = timeout_s
        self._lock = threading.Lock()
        self._access = ""
        self._secret = ""
        self._token = ""
        self._refresh_at = 0.0

    def resolve(self) -> tuple[str, str, str]:
        """返回 `(access_key_id, secret_access_key, session_token)`。"""
        with self._lock:
            if self._stale():
                self._refresh()
            return self._access, self._secret, self._token

    def stale(self) -> bool:
        with self._lock:
            return self._stale()

    def _stale(self) -> bool:
        return not self._access or time.monotonic() >= self._refresh_at

    def _refresh(self) -> None:
        try:
            with urllib.request.urlopen(self._url, timeout=self._timeout_s) as response:
                document = json.loads(response.read())
        except (OSError, ValueError) as exc:
            raise MetadataCredentialError(
                f"cannot fetch ECS metadata credentials from {self._url}: {exc}"
            ) from exc

        credential = document.get("credential") if isinstance(document, dict) else None
        if (
            not isinstance(credential, dict)
            or not credential.get("access")
            or not credential.get("secret")
        ):
            raise MetadataCredentialError(
                "ECS metadata returned no credential (is a cloud service agency attached?)"
            )

        expires_at = _parse_expiry(credential.get("expires_at"))
        ttl = (expires_at - time.time()) if expires_at is not None else 600.0
        # 至少留 60 秒可用期，避免 TTL 极短时每次调用都重取。
        refresh_in = max(ttl - self._refresh_margin_s, 60.0)

        self._access = str(credential["access"])
        self._secret = str(credential["secret"])
        self._token = str(credential.get("securitytoken") or "")
        self._refresh_at = time.monotonic() + refresh_in
