"""End-to-end integration tests. Require GEMINI_API_KEY."""

import os

import plotly.graph_objects as go
import pytest

from lava.engine.bootstrap import bootstrap
from lava.engine.query import execute_query
from lava.llm.pipeline import nl_to_vizspec
from lava.llm.sql_gen import vizspec_to_sql
from lava.viz.renderer import render

SALES_CSV = os.path.join(os.path.dirname(__file__), "..", "sample_data", "sales.csv")
HAS_API_KEY = bool(os.environ.get("OPENAI_API_KEY"))


@pytest.fixture
def lava_env(con):
    """Bootstrap the sales dataset and return (con, table_name, column_stats)."""
    table_name, column_stats = bootstrap(con, SALES_CSV, name="sales")
    return con, table_name, column_stats


def _run_query(lava_env, nl_query: str) -> go.Figure:
    con, table_name, column_stats = lava_env
    spec = nl_to_vizspec(nl_query, column_stats, use_cache=False)
    sql = vizspec_to_sql(spec, table_name)
    result = execute_query(con, sql)
    return render(spec, result, con)


@pytest.mark.skipif(not HAS_API_KEY, reason="GEMINI_API_KEY not set")
class TestEndToEnd:
    def test_revenue_by_region(self, lava_env):
        fig = _run_query(lava_env, "Show me total revenue by region")
        assert isinstance(fig, go.Figure)
        fig.to_json()

    def test_sales_trend(self, lava_env):
        fig = _run_query(lava_env, "How have sales changed over time?")
        assert isinstance(fig, go.Figure)

    def test_distribution(self, lava_env):
        fig = _run_query(lava_env, "What is the distribution of revenue?")
        assert isinstance(fig, go.Figure)

    def test_correlation(self, lava_env):
        fig = _run_query(lava_env, "Is there a relationship between quantity and revenue?")
        assert isinstance(fig, go.Figure)

    def test_top_products(self, lava_env):
        fig = _run_query(lava_env, "What are the top 10 products by revenue?")
        assert isinstance(fig, go.Figure)

    def test_filtered(self, lava_env):
        fig = _run_query(lava_env, "Show electronics sales in the West")
        assert isinstance(fig, go.Figure)


class TestBootstrap:
    def test_bootstrap_registers_dataset(self, con):
        from lava.engine.catalog import catalog_list
        table_name, column_stats = bootstrap(con, SALES_CSV, name="test_bootstrap")
        datasets = catalog_list(con)
        names = [d["name"] for d in datasets]
        assert "test_bootstrap" in names
        assert "revenue" in column_stats
        assert column_stats["revenue"]["kind"] == "numeric"


class TestApp:
    def test_app_layout_exists(self):
        from lava.viz.app import app
        assert app.layout is not None

    def test_app_has_expected_components(self):
        from lava.viz.app import app
        layout_str = str(app.layout)
        assert "nl-input" in layout_str
        assert "submit-btn" in layout_str
        assert "main-chart" in layout_str
