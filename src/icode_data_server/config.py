"""进程配置：全部来自环境变量，默认值保证 ``uv run`` 零外部依赖可启动。"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

MIB = 1024 * 1024
GIB = 1024 * MIB

DEFAULT_DEV_TOKEN = "dev-token"


@dataclass(frozen=True, slots=True)
class Principal:
    """由 token 决定的身份；客户端自报的任何身份字段一律忽略。"""

    tenant: str
    user_id: str


def _str(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value == "" else value


def _int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return int(value)


def _float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return float(value)


def _bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _tokens() -> dict[str, Principal]:
    raw = os.environ.get("ICODE_DS_TOKENS")
    if raw is None or raw.strip() == "":
        return {DEFAULT_DEV_TOKEN: Principal(tenant="dev", user_id="dev-user")}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("ICODE_DS_TOKENS must be a JSON object of token -> {tenant,user_id}")
    tokens: dict[str, Principal] = {}
    for token, value in parsed.items():
        if not isinstance(value, dict):
            raise ValueError("ICODE_DS_TOKENS entries must be objects")
        tenant = str(value.get("tenant") or "default")
        user_id = str(value.get("user_id") or "unknown")
        tokens[str(token)] = Principal(tenant=tenant, user_id=user_id)
    return tokens


@dataclass(frozen=True, slots=True)
class Settings:
    # 服务
    host: str = "127.0.0.1"
    port: int = 8080
    log_level: str = "INFO"

    # 认证
    tokens: dict[str, Principal] = field(
        default_factory=lambda: {DEFAULT_DEV_TOKEN: Principal(tenant="dev", user_id="dev-user")}
    )
    auth_disabled: bool = False

    # 存储后端
    repository_backend: str = "memory"  # memory | postgres
    object_store_backend: str = "local"  # local | s3
    postgres_dsn: str = ""
    postgres_auto_migrate: bool = True
    local_object_root: Path = Path("data/objects")

    s3_bucket: str = ""
    s3_endpoint_url: str = ""
    s3_region: str = ""
    s3_access_key_id: str = ""
    s3_secret_access_key: str = ""
    s3_prefix: str = ""

    # 限额（server-acceptance §2.1）
    max_body_bytes: int = 16 * MIB
    max_expanded_bytes: int = 64 * MIB
    max_frame_payload_bytes: int = 1 * MIB
    max_frames_per_batch: int = 4096
    batch_timeout_s: float = 30.0

    # 背压（§6.4）
    rate_limit_per_second: float = 25.0
    rate_limit_burst: float = 50.0
    max_inflight_batches_per_tenant: int = 32
    retry_after_ms: int = 1000
    rate_limit_retry_after_s: int = 1

    # 组装暂存（§3.2-4）
    staging_root: Path = Path("data/staging")
    staging_bytes_per_session: int = 64 * MIB
    staging_bytes_per_tenant: int = 2 * GIB
    assembly_ttl_s: float = 24 * 3600.0
    assembly_sweep_interval_s: float = 60.0

    # 派生器（§3.4.2）
    max_window_index_lines: int = 200
    max_window_bytes: int = 4 * MIB
    agent_env_max_bytes: int = 256 * 1024

    @classmethod
    def from_env(cls) -> Settings:
        # slots=True 的 dataclass 字段在类上是 member_descriptor，默认值只能从实例上取。
        d = cls()
        return cls(
            host=_str("ICODE_DS_HOST", d.host),
            port=_int("ICODE_DS_PORT", d.port),
            log_level=_str("ICODE_DS_LOG_LEVEL", d.log_level),
            tokens=_tokens(),
            auth_disabled=_bool("ICODE_DS_AUTH_DISABLED", d.auth_disabled),
            repository_backend=_str("REPOSITORY_BACKEND", d.repository_backend).lower(),
            object_store_backend=_str("OBJECT_STORE_BACKEND", d.object_store_backend).lower(),
            postgres_dsn=_str("ICODE_DS_POSTGRES_DSN", d.postgres_dsn),
            postgres_auto_migrate=_bool("ICODE_DS_POSTGRES_AUTO_MIGRATE", d.postgres_auto_migrate),
            local_object_root=Path(_str("ICODE_DS_OBJECT_ROOT", str(d.local_object_root))),
            s3_bucket=_str("ICODE_DS_S3_BUCKET", d.s3_bucket),
            s3_endpoint_url=_str("ICODE_DS_S3_ENDPOINT_URL", d.s3_endpoint_url),
            s3_region=_str("ICODE_DS_S3_REGION", d.s3_region),
            s3_access_key_id=_str("ICODE_DS_S3_ACCESS_KEY_ID", d.s3_access_key_id),
            s3_secret_access_key=_str("ICODE_DS_S3_SECRET_ACCESS_KEY", d.s3_secret_access_key),
            s3_prefix=_str("ICODE_DS_S3_PREFIX", d.s3_prefix),
            max_body_bytes=_int("ICODE_DS_MAX_BODY_BYTES", d.max_body_bytes),
            max_expanded_bytes=_int("ICODE_DS_MAX_EXPANDED_BYTES", d.max_expanded_bytes),
            max_frame_payload_bytes=_int(
                "ICODE_DS_MAX_FRAME_PAYLOAD_BYTES", d.max_frame_payload_bytes
            ),
            max_frames_per_batch=_int("ICODE_DS_MAX_FRAMES_PER_BATCH", d.max_frames_per_batch),
            batch_timeout_s=_float("ICODE_DS_BATCH_TIMEOUT_S", d.batch_timeout_s),
            rate_limit_per_second=_float("ICODE_DS_RATE_LIMIT_PER_SECOND", d.rate_limit_per_second),
            rate_limit_burst=_float("ICODE_DS_RATE_LIMIT_BURST", d.rate_limit_burst),
            max_inflight_batches_per_tenant=_int(
                "ICODE_DS_MAX_INFLIGHT_BATCHES", d.max_inflight_batches_per_tenant
            ),
            retry_after_ms=_int("ICODE_DS_RETRY_AFTER_MS", d.retry_after_ms),
            rate_limit_retry_after_s=_int(
                "ICODE_DS_RATE_LIMIT_RETRY_AFTER_S", d.rate_limit_retry_after_s
            ),
            staging_root=Path(_str("ICODE_DS_STAGING_ROOT", str(d.staging_root))),
            staging_bytes_per_session=_int(
                "ICODE_DS_STAGING_BYTES_PER_SESSION", d.staging_bytes_per_session
            ),
            staging_bytes_per_tenant=_int(
                "ICODE_DS_STAGING_BYTES_PER_TENANT", d.staging_bytes_per_tenant
            ),
            assembly_ttl_s=_float("ICODE_DS_ASSEMBLY_TTL_S", d.assembly_ttl_s),
            assembly_sweep_interval_s=_float(
                "ICODE_DS_ASSEMBLY_SWEEP_INTERVAL_S", d.assembly_sweep_interval_s
            ),
            max_window_index_lines=_int(
                "ICODE_DS_MAX_WINDOW_INDEX_LINES", d.max_window_index_lines
            ),
            max_window_bytes=_int("ICODE_DS_MAX_WINDOW_BYTES", d.max_window_bytes),
            agent_env_max_bytes=_int("ICODE_DS_AGENT_ENV_MAX_BYTES", d.agent_env_max_bytes),
        )

    def principal_for_token(self, token: str) -> Principal | None:
        return self.tokens.get(token)

    @property
    def anonymous_principal(self) -> Principal:
        return Principal(tenant="default", user_id="anonymous")
