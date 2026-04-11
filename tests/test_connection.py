"""Tests for DuckDB connection management."""

import duckdb

from lava.engine.connection import get_connection, reset_connection


class TestConnection:
    def setup_method(self):
        reset_connection()

    def teardown_method(self):
        reset_connection()

    def test_connection_returns_duckdb(self):
        con = get_connection()
        assert isinstance(con, duckdb.DuckDBPyConnection)

    def test_connection_is_singleton(self):
        con1 = get_connection()
        con2 = get_connection()
        assert con1 is con2

    def test_connection_object_cache_enabled(self):
        con = get_connection()
        result = con.execute(
            "SELECT current_setting('enable_object_cache')"
        ).fetchone()[0]
        assert result is True or result == "true"
