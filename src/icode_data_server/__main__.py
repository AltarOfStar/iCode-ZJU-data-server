"""命令行入口：`python -m icode_data_server [--self-check]`。"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from . import __version__
from .config import Settings
from .logging_setup import configure_logging


def _self_check() -> int:
    from dataclasses import replace

    import httpx

    from .app import create_app

    settings = replace(
        Settings.from_env(),
        repository_backend="memory",
        object_store_backend="local",
        auth_disabled=False,
    )
    configure_logging(settings.log_level)
    app = create_app(settings)

    async def run() -> dict:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://self") as client:
            response = await client.get("/healthz")
        return {"status_code": response.status_code, "body": response.json()}

    result = asyncio.run(run())
    print(json.dumps({"version": __version__, "healthz": result}, ensure_ascii=False))
    return 0 if result["status_code"] == 200 else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="icode-data-server")
    parser.add_argument("--self-check", action="store_true", help="进程内冒烟检查后退出")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args(argv)

    if args.self_check:
        return _self_check()

    import uvicorn

    settings = Settings.from_env()
    configure_logging(settings.log_level)
    uvicorn.run(
        "icode_data_server.app:create_app",
        factory=True,
        host=args.host or settings.host,
        port=args.port or settings.port,
        reload=args.reload,
        log_config=None,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
