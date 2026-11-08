"""FastAPI 应用装配。"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import __version__
from .api import router
from .config import Settings
from .logging_setup import configure_logging
from .services import Services

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    configure_logging(settings.log_level)
    services = Services(settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        await services.start()
        try:
            yield
        finally:
            await services.close()

    app = FastAPI(
        title="iCode data server",
        version=__version__,
        description="Fact-reporting ingest service (report/1.0)",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.services = services
    app.include_router(router)
    logger.info(
        "app_created",
        extra={
            "event": "app_created",
            "repository_backend": settings.repository_backend,
            "object_store_backend": settings.object_store_backend,
        },
    )
    return app
