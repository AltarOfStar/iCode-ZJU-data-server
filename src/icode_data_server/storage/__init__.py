"""存储装配：按配置选择 ObjectStore 与 Repository 后端。"""

from __future__ import annotations

from ..config import Settings
from .models import (
    OBJECT_MISSING,
    OBJECT_PENDING,
    OBJECT_SUPERSEDED,
    OBJECT_VERIFIED,
    RECEIPT_COMMITTED,
    RECEIPT_PENDING,
    RECEIPT_REJECTED,
    RECEIPT_UNSUPPORTED,
    TERMINAL_RECEIPT_STATES,
    BatchRecord,
    EventRecord,
    ObjectRecord,
    ReceiptRecord,
    RepositoryRecord,
    SessionAnchor,
    SessionRecord,
    TurnRecord,
    WorkspaceStateRecord,
)
from .objects import LocalObjectStore, ObjectStore, S3ObjectStore, object_key
from .repository import MemoryRepository, Repository


def build_object_store(settings: Settings) -> ObjectStore:
    backend = settings.object_store_backend
    if backend == "local":
        return LocalObjectStore(settings.local_object_root)
    if backend == "s3":
        return S3ObjectStore(
            bucket=settings.s3_bucket,
            endpoint_url=settings.s3_endpoint_url,
            region=settings.s3_region,
            access_key_id=settings.s3_access_key_id,
            secret_access_key=settings.s3_secret_access_key,
            prefix=settings.s3_prefix,
        )
    raise ValueError(f"unknown OBJECT_STORE_BACKEND: {backend}")


def build_repository(settings: Settings) -> Repository:
    backend = settings.repository_backend
    if backend == "memory":
        return MemoryRepository()
    if backend == "postgres":
        from .postgres import PostgresRepository

        if not settings.postgres_dsn:
            raise ValueError("REPOSITORY_BACKEND=postgres requires ICODE_DS_POSTGRES_DSN")
        return PostgresRepository(
            settings.postgres_dsn, auto_migrate=settings.postgres_auto_migrate
        )
    raise ValueError(f"unknown REPOSITORY_BACKEND: {backend}")


__all__ = [
    "OBJECT_MISSING",
    "OBJECT_PENDING",
    "OBJECT_SUPERSEDED",
    "OBJECT_VERIFIED",
    "RECEIPT_COMMITTED",
    "RECEIPT_PENDING",
    "RECEIPT_REJECTED",
    "RECEIPT_UNSUPPORTED",
    "TERMINAL_RECEIPT_STATES",
    "BatchRecord",
    "EventRecord",
    "LocalObjectStore",
    "MemoryRepository",
    "ObjectRecord",
    "ObjectStore",
    "ReceiptRecord",
    "Repository",
    "RepositoryRecord",
    "S3ObjectStore",
    "SessionAnchor",
    "SessionRecord",
    "TurnRecord",
    "WorkspaceStateRecord",
    "build_object_store",
    "build_repository",
    "object_key",
]
