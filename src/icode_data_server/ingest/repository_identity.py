"""`repo.identity` 载荷校验（`repository/1.0`，server-acceptance §3 + client-design §4.3）。

只登记 host/path 明文 + 摘要；无远端或多 remote 无 origin 的情形由客户端决定不上报。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

REPOSITORY_SCHEMA = "repository/1.0"
CONNECTION_ID = "report"
RESOLUTION = "reported"


class RepositoryIdentityError(ValueError):
    """载荷不符合 `repository/1.0` → `schema_invalid`。"""


@dataclass(frozen=True, slots=True)
class RepositoryIdentity:
    identity: str
    host: str
    path: str
    url_kind: str
    digest: str | None
    observed_at: str | None

    @property
    def repository_key(self) -> str:
        return repository_key(self.host, self.path)

    @property
    def provider(self) -> str:
        return provider_of(self.host)


def parse_repository_identity(payload: bytes) -> RepositoryIdentity:
    try:
        document = json.loads(payload)
    except (ValueError, UnicodeDecodeError) as exc:
        raise RepositoryIdentityError(f"repo.identity payload is not valid JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise RepositoryIdentityError("repo.identity payload must be a JSON object")
    schema = document.get("schema")
    if schema is not None and schema != REPOSITORY_SCHEMA:
        raise RepositoryIdentityError(f"unsupported repository schema: {schema!r}")
    identity = document.get("repositoryIdentity")
    if not isinstance(identity, str) or not identity:
        raise RepositoryIdentityError("repo.identity payload has no repositoryIdentity")
    remote = document.get("remote")
    if not isinstance(remote, dict):
        raise RepositoryIdentityError("repo.identity payload has no remote")
    host = remote.get("host")
    path = remote.get("path")
    if not isinstance(host, str) or not host:
        raise RepositoryIdentityError("repo.identity remote.host is required")
    if not isinstance(path, str) or not path:
        raise RepositoryIdentityError("repo.identity remote.path is required")
    url_kind = remote.get("urlKind")
    return RepositoryIdentity(
        identity=identity,
        host=host,
        path=path.lstrip("/"),
        url_kind=url_kind if isinstance(url_kind, str) and url_kind else "unknown",
        digest=document.get("remoteDigest")
        if isinstance(document.get("remoteDigest"), str)
        else None,
        observed_at=document.get("observedAt")
        if isinstance(document.get("observedAt"), str)
        else None,
    )


def repository_key(host: str, path: str) -> str:
    return hashlib.sha256(f"{host.lower()}/{path}".encode()).hexdigest()


def provider_of(host: str) -> str:
    lowered = host.lower()
    if "github" in lowered:
        return "github"
    if "gitlab" in lowered:
        return "gitlab"
    if "gitee" in lowered:
        return "gitee"
    if "codehub" in lowered or "devcloud" in lowered:
        return "codehub"
    return "generic"
