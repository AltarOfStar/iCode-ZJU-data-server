"""认证：`Authorization: Bearer <token>` → `(tenant, user_id)`（§2.1）。

客户端自报的任何身份字段一律忽略；缺失/无效 token → `401`。
"""

from __future__ import annotations

from fastapi import HTTPException, Request, status

from ..config import Principal


def bearer_principal(request: Request) -> Principal:
    settings = request.app.state.settings
    header = request.headers.get("authorization", "")
    if header.lower().startswith("bearer "):
        principal = settings.principal_for_token(header[7:].strip())
        if principal is not None:
            return principal
    if settings.auth_disabled:
        return settings.anonymous_principal
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="missing or invalid bearer token",
        headers={"WWW-Authenticate": "Bearer"},
    )
