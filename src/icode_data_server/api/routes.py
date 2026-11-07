"""路由：`/healthz`、`POST /v1/batch`、`POST /v1/session/{session_id}/manifest`。"""

from __future__ import annotations

import asyncio
import logging
import zlib

from fastapi import APIRouter, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse

from .. import __version__
from ..config import Settings
from ..frames import FrameError, decode_frames
from ..ingest.manifest import ManifestRequestError, parse_manifest_request
from .auth import bearer_principal

logger = logging.getLogger(__name__)

router = APIRouter()

_MAX_MANIFEST_BODY = 1 * 1024 * 1024


@router.get("/healthz")
async def healthz(request: Request) -> dict:
    return {
        "status": "ok",
        "version": __version__,
        "repository_backend": request.app.state.settings.repository_backend,
        "object_store_backend": request.app.state.settings.object_store_backend,
    }


@router.post("/v1/batch")
async def post_batch(request: Request) -> Response:
    settings = request.app.state.settings
    principal = bearer_principal(request)
    services = request.app.state.services
    await services.start()
    processor = services.require_processor()

    # §6.4-1：闸门在读体之前拒绝，避免 64 MiB 解压缓冲先被分配出去。
    if not services.gate.try_enter():
        return _overloaded(settings)
    try:
        if not processor.admit(principal):
            return _rate_limited(settings)

        body = await _read_limited(request, settings.max_body_bytes)
        body = _maybe_decompress(request, body, settings.max_expanded_bytes)
        try:
            frames = decode_frames(body)
        except FrameError as exc:
            logger.warning(
                "batch_frame_error",
                extra={
                    "event": "batch_frame_error",
                    "tenant": principal.tenant,
                    "reason": str(exc),
                },
            )
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"error": "frame_format", "message": str(exc)},
            )
        if len(frames) > settings.max_frames_per_batch:
            return JSONResponse(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                content={
                    "error": "too_many_frames",
                    "frames": len(frames),
                    "limit": settings.max_frames_per_batch,
                },
            )

        try:
            result = await asyncio.wait_for(
                processor.process(principal, frames), timeout=settings.batch_timeout_s
            )
        except TimeoutError:
            logger.error(
                "batch_timeout",
                extra={"event": "batch_timeout", "tenant": principal.tenant, "frames": len(frames)},
            )
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error": "batch_timeout"},
                headers={"Retry-After": "1"},
            )

        logger.info(
            "batch_processed",
            extra={
                "event": "batch_processed",
                "tenant": principal.tenant,
                "frames": len(frames),
                "accepted": len(result.accepted),
                "duplicates": len(result.duplicates),
                "rejected": len(result.rejected),
                "deferred": len(result.deferred),
                "retry_after_ms": result.retry_after_ms,
            },
        )
        return JSONResponse(status_code=status.HTTP_200_OK, content=result.as_response())
    finally:
        services.gate.leave()


@router.post("/v1/session/{session_id}/manifest")
async def post_manifest(session_id: str, request: Request) -> Response:
    settings = request.app.state.settings
    principal = bearer_principal(request)
    services = request.app.state.services
    await services.start()
    manifest = services.require_manifest()

    if not services.gate.try_enter():
        return _overloaded(settings)
    try:
        body = await _read_limited(request, _MAX_MANIFEST_BODY)
        try:
            parsed = parse_manifest_request(body)
        except ManifestRequestError as exc:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"error": "manifest_format", "message": str(exc)},
            )
        response = await manifest.reconcile(
            tenant=principal.tenant, session_id=session_id, request=parsed
        )
        logger.info(
            "manifest_reconciled",
            extra={
                "event": "manifest_reconciled",
                "tenant": principal.tenant,
                "session_id": session_id,
                "missing_chunks": len(response["missing_chunk_hashes"]),
                "missing_files": len(response["missing_file_shas"]),
                "known_snapshot": response["known_snapshot"],
            },
        )
        return JSONResponse(status_code=status.HTTP_200_OK, content=response)
    finally:
        services.gate.leave()


def _overloaded(settings: Settings) -> JSONResponse:
    retry_after = settings.rate_limit_retry_after_s
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"error": "overloaded", "retry_after_ms": retry_after * 1000},
        headers={"Retry-After": str(retry_after)},
    )


def _rate_limited(settings: Settings) -> JSONResponse:
    retry_after = settings.rate_limit_retry_after_s
    return JSONResponse(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        content={"error": "rate_limited", "retry_after_ms": retry_after * 1000},
        headers={"Retry-After": str(retry_after)},
    )


async def _read_limited(request: Request, limit: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail=f"request body exceeds {limit} bytes",
            )
        chunks.append(chunk)
    return b"".join(chunks)


def _maybe_decompress(request: Request, body: bytes, limit: int) -> bytes:
    encoding = request.headers.get("content-encoding", "").strip().lower()
    if encoding not in {"gzip", "deflate"}:
        if len(body) > limit:
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail=f"request body exceeds {limit} bytes",
            )
        return body
    wbits = 16 + zlib.MAX_WBITS if encoding == "gzip" else zlib.MAX_WBITS
    decompressor = zlib.decompressobj(wbits)
    try:
        expanded = decompressor.decompress(body, limit + 1)
    except zlib.error as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"invalid {encoding} body: {exc}"
        ) from exc
    if len(expanded) > limit or decompressor.unconsumed_tail:
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail=f"decompressed body exceeds {limit} bytes",
        )
    return expanded
