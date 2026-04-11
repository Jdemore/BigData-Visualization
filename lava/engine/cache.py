"""Query cache — LRU with byte-budget eviction."""

import hashlib
from collections import OrderedDict

from lava.engine.query import DataResult


class QueryCache:
    """LRU cache for DataResult objects keyed by SQL hash. Evicts by byte budget."""

    def __init__(self, max_entries: int = 64, max_bytes: int = 500_000_000) -> None:
        self._cache: OrderedDict[str, DataResult] = OrderedDict()
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._current_bytes = 0

    def _key(self, sql: str) -> str:
        return hashlib.sha256(sql.strip().encode()).hexdigest()

    def get(self, sql: str) -> DataResult | None:
        k = self._key(sql)
        if k in self._cache:
            self._cache.move_to_end(k)
            return self._cache[k]
        return None

    def put(self, sql: str, result: DataResult) -> None:
        k = self._key(sql)
        nbytes = result.arrow_table.nbytes
        while self._current_bytes + nbytes > self._max_bytes and self._cache:
            _, evicted = self._cache.popitem(last=False)
            self._current_bytes -= evicted.arrow_table.nbytes
        if len(self._cache) >= self._max_entries:
            _, evicted = self._cache.popitem(last=False)
            self._current_bytes -= evicted.arrow_table.nbytes
        self._cache[k] = result
        self._current_bytes += nbytes
