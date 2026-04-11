"""Tests for query execution."""

import pyarrow as pa

from lava.engine.query import execute_query


class TestDataResult:
    def test_dataresult_fields(self, con):
        result = execute_query(con, "SELECT 1 AS x")
        assert hasattr(result, "arrow_table")
        assert hasattr(result, "row_count")
        assert hasattr(result, "query_ms")
        assert hasattr(result, "sql")
        assert hasattr(result, "warnings")


class TestExecuteQuery:
    def test_basic_query(self, con):
        result = execute_query(con, "SELECT 1 AS x")
        assert result.row_count == 1

    def test_returns_arrow_table(self, con):
        result = execute_query(con, "SELECT 1 AS x")
        assert isinstance(result.arrow_table, pa.Table)

    def test_timing_positive(self, con):
        result = execute_query(con, "SELECT 1 AS x")
        assert result.query_ms > 0

    def test_limit_warning_on_large_result(self, con):
        con.execute("""
            CREATE TABLE big AS
            SELECT i FROM generate_series(1, 200000) AS t(i)
        """)
        result = execute_query(con, "SELECT * FROM big", max_viz_rows=100_000)
        assert len(result.warnings) > 0
        assert result.row_count <= 100_000
