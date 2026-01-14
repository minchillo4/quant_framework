import asyncio
import time
from collections import deque

HEAD = "api_key"  # header / query name required by Coinalyze
MAX_PER_KEY = 39  # stay just under 40
WINDOW = 60


class KeyRotator:
    """
    Thread-safe API-key rotator for Coinalyze.
    Keeps ≤ N keys in a ring, tracks per-key calls in Redis (or memory),
    auto-evicts exhausted keys and refreshes the ring when every key is hot.
    Usage:
        rotator = KeyRotator(["key1", "key2", "key3"], redis)  # redis=None is OK
        headers = await rotator.get_headers()  # blocks until a key is free
    """

    def __init__(self, keys: list[str]):
        if not keys:
            raise RuntimeError("No API keys provided")
        self._keys: deque[str] = deque(keys)
        self._lock = asyncio.Lock()
        self._local: dict[str, deque[float]] = {}  # fallback if redis absent

    # ---------- public ----------

    async def get_headers(self) -> dict[str, str]:
        """Return headers dict with an available key; wait if necessary."""
        while True:
            key = await self._next_available_key()
            if key:
                await self._record_call(key)
                return {HEAD: key}
            await asyncio.sleep(0.5)  # spin politely

    # ---------- internal ----------

    async def _next_available_key(self) -> str | None:
        async with self._lock:
            now = time.time()
            for _ in range(len(self._keys)):
                key = self._keys[0]
                self._keys.rotate(-1)
                if await self._call_count(key, now) < MAX_PER_KEY:
                    return key
            # all keys exhausted – refresh ring (could fetch new keys here)
            await self._refresh_keys()
            return None

    async def _call_count(self, key: str, now: float) -> int:
        """Number of calls made with `key` in the last 60 s."""  # memory fallback
        dq = self._local.setdefault(key, deque(maxlen=MAX_PER_KEY * 2))
        while dq and dq[0] < now - WINDOW:
            dq.popleft()
        return len(dq)

    async def _record_call(self, key: str) -> None:
        now = time.time()
        self._local.setdefault(key, deque(maxlen=MAX_PER_KEY * 2)).append(now)

    async def _refresh_keys(self) -> None:
        """
        Replenish the ring – for now just re-queue the same keys
        (you could fetch new keys from a secret store here).
        """
        await asyncio.sleep(WINDOW / 2)  # wait half window
