"""按键串行化的小工具：同一会话的写入必须串行（server-acceptance §3.5-2）。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Hashable
from contextlib import asynccontextmanager


class KeyedLocks:
    def __init__(self) -> None:
        self._guard = asyncio.Lock()
        self._locks: dict[Hashable, tuple[asyncio.Lock, int]] = {}

    @asynccontextmanager
    async def hold(self, key: Hashable) -> AsyncIterator[None]:
        async with self._guard:
            lock, users = self._locks.get(key, (asyncio.Lock(), 0))
            self._locks[key] = (lock, users + 1)
        try:
            async with lock:
                yield
        finally:
            async with self._guard:
                current_lock, current_users = self._locks.get(key, (lock, 1))
                if current_users <= 1:
                    self._locks.pop(key, None)
                else:
                    self._locks[key] = (current_lock, current_users - 1)
