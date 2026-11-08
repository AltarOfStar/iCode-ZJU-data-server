"""背压：请求闸门 + per-user/per-tenant 令牌桶 + 并发批次计数（server-acceptance §6.4）。

- 请求闸门满 → API 层 `503` + `Retry-After`（在读体之前拒绝，保护内存）；
- 令牌桶超限 → `429` + `Retry-After`（单用户桶防单点刷量，租户桶是总闸）；
- 单租户并发批次超限 → 整批 `deferred` + `retry_after_ms`（无副作用，客户端退避重发）。
"""

from __future__ import annotations

import time


class TokenBucket:
    def __init__(self, *, rate_per_second: float, burst: float, clock=time.monotonic) -> None:
        self._rate = max(rate_per_second, 0.0)
        self._burst = max(burst, 1.0)
        self._clock = clock
        self._buckets: dict[str, tuple[float, float]] = {}

    def take(self, key: str) -> bool:
        if self._rate <= 0:
            return True
        now = self._clock()
        tokens, updated = self._buckets.get(key, (self._burst, now))
        tokens = min(self._burst, tokens + (now - updated) * self._rate)
        if tokens < 1.0:
            self._buckets[key] = (tokens, now)
            return False
        self._buckets[key] = (tokens - 1.0, now)
        return True

    def retry_after_seconds(self) -> int:
        if self._rate <= 0:
            return 0
        return max(1, int(round(1.0 / self._rate)))


class RequestGate:
    """进程级并发闸门：在读取请求体之前限制同时在处理的请求数（§6.4-1）。

    计数只在事件循环内同步增减（中间没有 ``await``），因此不需要额外加锁。
    闸门满了立即拒绝，不排队——排队会把内存问题推迟到读体之后。
    """

    def __init__(self, *, max_concurrent: int) -> None:
        self._max_concurrent = max(int(max_concurrent), 1)
        self._inflight = 0

    def try_enter(self) -> bool:
        if self._inflight >= self._max_concurrent:
            return False
        self._inflight += 1
        return True

    def leave(self) -> None:
        if self._inflight > 0:
            self._inflight -= 1

    @property
    def inflight(self) -> int:
        return self._inflight


class TenantGuard:
    def __init__(self, *, max_inflight: int) -> None:
        self._max_inflight = max(int(max_inflight), 1)
        self._inflight: dict[str, int] = {}

    def enter(self, tenant: str) -> bool:
        current = self._inflight.get(tenant, 0)
        if current >= self._max_inflight:
            return False
        self._inflight[tenant] = current + 1
        return True

    def leave(self, tenant: str) -> None:
        current = self._inflight.get(tenant, 0)
        if current <= 1:
            self._inflight.pop(tenant, None)
        else:
            self._inflight[tenant] = current - 1

    def inflight(self, tenant: str) -> int:
        return self._inflight.get(tenant, 0)
