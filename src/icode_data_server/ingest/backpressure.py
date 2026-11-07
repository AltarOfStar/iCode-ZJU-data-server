"""背压：per-tenant 令牌桶 + 并发批次计数（server-acceptance §6.4）。

- 请求速率超限 → `429` + `Retry-After`（由 API 层转成响应）；
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
