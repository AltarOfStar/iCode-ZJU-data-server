"""服务装配：配置 → 存储后端 → 处理器，供 API 层与冒烟检查共用。"""

from __future__ import annotations

import asyncio
import logging

from .config import Settings
from .ingest.assembly import AssemblyStore
from .ingest.manifest import ManifestService
from .ingest.processor import BatchProcessor
from .ingest.trajectory import TrajectoryStore
from .storage import build_object_store, build_repository

logger = logging.getLogger(__name__)


class Services:
    """进程级容器；`start()` 幂等，便于 ASGITransport 测试（不依赖 lifespan 事件）。"""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.repository = build_repository(settings)
        self.objects = build_object_store(settings)
        self.assemblies = AssemblyStore(
            settings.staging_root,
            per_session_bytes=settings.staging_bytes_per_session,
            per_tenant_bytes=settings.staging_bytes_per_tenant,
            ttl_s=settings.assembly_ttl_s,
        )
        self.trajectory: TrajectoryStore | None = None
        self.processor: BatchProcessor | None = None
        self.manifest: ManifestService | None = None
        self._lock = asyncio.Lock()
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        async with self._lock:
            if self._started:
                return
            await self.repository.start()
            await self.objects.start()
            self.trajectory = TrajectoryStore(
                self.repository,
                self.objects,
                max_index_lines=self.settings.max_window_index_lines,
                max_window_bytes=self.settings.max_window_bytes,
            )
            self.processor = BatchProcessor(
                settings=self.settings,
                repository=self.repository,
                objects=self.objects,
                assemblies=self.assemblies,
                trajectory=self.trajectory,
            )
            self.manifest = ManifestService(self.repository, self.assemblies, self.trajectory)
            self._started = True
            logger.info(
                "services_started",
                extra={
                    "event": "services_started",
                    "repository_backend": self.settings.repository_backend,
                    "object_store_backend": self.settings.object_store_backend,
                },
            )

    async def close(self) -> None:
        if not self._started:
            return
        await self.objects.close()
        await self.repository.close()
        self._started = False

    def require_processor(self) -> BatchProcessor:
        assert self.processor is not None, "services are not started"
        return self.processor

    def require_manifest(self) -> ManifestService:
        assert self.manifest is not None, "services are not started"
        return self.manifest
