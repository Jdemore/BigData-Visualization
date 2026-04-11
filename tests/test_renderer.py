"""Tests for visualization renderer."""

import plotly.graph_objects as go

from lava.engine.query import execute_query
from lava.llm.schema import VizSpec
from lava.viz.renderer import render


def _spec(**kwargs) -> VizSpec:
    defaults = dict(
        intent="explore",
        x_axis={"column": "region", "aggregation": None},
        y_axis={"column": "revenue", "aggregation": None},
        color_by=None, filters=None, sort_by=None,
        limit=None, chart_type="table", title="Test",
    )
    defaults.update(kwargs)
    return VizSpec(**defaults)


class TestRenderer:
    def test_render_bar(self, sales_table):
        result = execute_query(sales_table, 'SELECT region, revenue FROM test_sales')
        spec = _spec(chart_type="bar")
        fig = render(spec, result)
        assert isinstance(fig, go.Figure)
        fig.to_json()

    def test_render_line(self, sales_table):
        result = execute_query(sales_table, 'SELECT date, revenue FROM test_sales')
        spec = _spec(
            x_axis={"column": "date", "aggregation": None},
            y_axis={"column": "revenue", "aggregation": None},
            chart_type="line",
        )
        fig = render(spec, result)
        assert isinstance(fig, go.Figure)

    def test_render_scatter(self, sales_table):
        result = execute_query(sales_table, 'SELECT quantity, revenue FROM test_sales')
        spec = _spec(
            x_axis={"column": "quantity", "aggregation": None},
            y_axis={"column": "revenue", "aggregation": None},
            chart_type="scatter",
        )
        fig = render(spec, result)
        assert isinstance(fig, go.Figure)

    def test_render_histogram(self, sales_table):
        result = execute_query(sales_table, 'SELECT revenue FROM test_sales')
        spec = _spec(
            x_axis={"column": "revenue", "aggregation": None},
            y_axis=None, chart_type="histogram",
        )
        fig = render(spec, result)
        assert isinstance(fig, go.Figure)

    def test_render_pie(self, sales_table):
        result = execute_query(
            sales_table,
            'SELECT region, SUM(revenue) AS total FROM test_sales GROUP BY region',
        )
        spec = _spec(
            x_axis={"column": "region", "aggregation": None},
            y_axis={"column": "total", "aggregation": None},
            chart_type="pie",
        )
        fig = render(spec, result)
        assert isinstance(fig, go.Figure)

    def test_render_table(self, sales_table):
        result = execute_query(sales_table, 'SELECT * FROM test_sales')
        spec = _spec(chart_type="table")
        fig = render(spec, result)
        assert isinstance(fig, go.Figure)
        assert any(isinstance(t, go.Table) for t in fig.data)

    def test_render_table_truncates(self, sales_table):
        sales_table.execute("""
            CREATE TABLE big_sales AS
            SELECT * FROM test_sales UNION ALL SELECT * FROM test_sales
        """)
        for _ in range(3):
            sales_table.execute("INSERT INTO big_sales SELECT * FROM big_sales")
        result = execute_query(sales_table, 'SELECT * FROM big_sales')
        spec = _spec(chart_type="table")
        fig = render(spec, result)
        table_trace = [t for t in fig.data if isinstance(t, go.Table)][0]
        for col_values in table_trace.cells.values:
            assert len(col_values) <= 1000


class TestReduction:
    def test_bin_sql_valid(self, sales_table):
        from lava.viz.reduction import _bin_sql
        sql = _bin_sql("SELECT * FROM test_sales", _spec(
            x_axis={"column": "revenue", "aggregation": None},
            y_axis=None,
        ))
        result = sales_table.execute(sql).fetchall()
        assert len(result) > 0

    def test_aggregate_sql_valid(self, sales_table):
        from lava.viz.reduction import _aggregate_sql
        spec = _spec(
            x_axis={"column": "region", "aggregation": None},
            y_axis={"column": "revenue", "aggregation": "sum"},
            color_by="region",
        )
        sql = _aggregate_sql("SELECT * FROM test_sales", spec)
        result = sales_table.execute(sql).fetchall()
        assert len(result) > 0

    def test_lttb_sql_valid(self, sales_table):
        from lava.viz.reduction import _lttb_sql
        sql = _lttb_sql("SELECT * FROM test_sales", target=50)
        result = sales_table.execute(sql).fetchall()
        assert len(result) <= 50
