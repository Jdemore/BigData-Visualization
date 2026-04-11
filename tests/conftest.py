"""Shared test fixtures for LAVA."""

import duckdb
import pytest


@pytest.fixture
def con():
    """Fresh in-memory DuckDB connection per test."""
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


@pytest.fixture
def sales_table(con):
    """Small test table with ~100 rows of sales-like data."""
    con.execute("""
        CREATE TABLE test_sales AS
        SELECT
            i AS order_id,
            '2024-01-' || LPAD(CAST(1 + (i % 28) AS VARCHAR), 2, '0') AS date,
            CASE i % 5
                WHEN 0 THEN 'Northeast'
                WHEN 1 THEN 'Southeast'
                WHEN 2 THEN 'Midwest'
                WHEN 3 THEN 'West'
                ELSE 'Southwest'
            END AS region,
            CASE i % 4
                WHEN 0 THEN 'Electronics'
                WHEN 1 THEN 'Clothing'
                WHEN 2 THEN 'Home'
                ELSE 'Sports'
            END AS product_category,
            ROUND(RANDOM() * 100 + 1, 2) AS quantity,
            ROUND(RANDOM() * 500 + 10, 2) AS unit_price,
            ROUND((RANDOM() * 100 + 1) * (RANDOM() * 500 + 10), 2) AS revenue,
            'C' || LPAD(CAST(100 + (i % 50) AS VARCHAR), 3, '0') AS customer_id
        FROM generate_series(1, 100) AS t(i)
    """)
    return con
