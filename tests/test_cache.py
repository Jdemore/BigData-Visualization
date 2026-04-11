"""Tests for query cache."""

from lava.engine.cache import QueryCache
from lava.engine.query import execute_query


class TestQueryCache:
    def test_cache_hit(self, con):
        cache = QueryCache()
        result = execute_query(con, "SELECT 1 AS x")
        cache.put("SELECT 1 AS x", result)
        cached = cache.get("SELECT 1 AS x")
        assert cached is not None
        assert cached.row_count == 1

    def test_cache_miss(self):
        cache = QueryCache()
        assert cache.get("SELECT 999") is None

    def test_lru_eviction(self, con):
        cache = QueryCache(max_entries=2)
        r1 = execute_query(con, "SELECT 1 AS x")
        r2 = execute_query(con, "SELECT 2 AS x")
        r3 = execute_query(con, "SELECT 3 AS x")
        cache.put("q1", r1)
        cache.put("q2", r2)
        cache.put("q3", r3)
        # q1 should be evicted
        assert cache.get("q1") is None
        assert cache.get("q2") is not None
        assert cache.get("q3") is not None

    def test_byte_eviction(self, con):
        # Create a result with known size, set tiny byte budget
        result = execute_query(con, "SELECT i FROM generate_series(1, 1000) AS t(i)")
        cache = QueryCache(max_entries=100, max_bytes=1)
        cache.put("big", result)
        # Cache should have evicted everything before adding, or just have this one
        # The point is it doesn't crash
        assert cache.get("big") is not None
