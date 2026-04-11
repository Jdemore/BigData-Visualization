"""Tests for VizSpec to SQL generation."""

from lava.llm.schema import VizSpec
from lava.llm.sql_gen import vizspec_to_sql


def _spec(**kwargs) -> VizSpec:
    defaults = dict(
        intent="explore",
        x_axis={"column": "revenue", "aggregation": None},
        y_axis=None, color_by=None, filters=None,
        sort_by=None, limit=None, chart_type="table", title="Test",
    )
    defaults.update(kwargs)
    return VizSpec(**defaults)


class TestSqlGen:
    def test_basic_select(self):
        sql = vizspec_to_sql(_spec(), "sales")
        assert '"revenue"' in sql
        assert 'FROM "sales"' in sql

    def test_with_y_axis(self):
        spec = _spec(
            x_axis={"column": "region", "aggregation": None},
            y_axis={"column": "revenue", "aggregation": "sum"},
        )
        sql = vizspec_to_sql(spec, "sales")
        assert 'SUM("revenue")' in sql
        assert 'GROUP BY "region"' in sql

    def test_with_filters_numeric(self):
        spec = _spec(filters=[{"column": "revenue", "op": ">", "value": 100}])
        sql = vizspec_to_sql(spec, "sales")
        assert '"revenue" > 100' in sql

    def test_with_filters_string(self):
        spec = _spec(filters=[{"column": "region", "op": "==", "value": "West"}])
        sql = vizspec_to_sql(spec, "sales")
        assert '"region" == \'West\'' in sql

    def test_contains_filter(self):
        spec = _spec(filters=[{"column": "region", "op": "contains", "value": "east"}])
        sql = vizspec_to_sql(spec, "sales")
        assert "ILIKE '%east%'" in sql

    def test_with_color_by(self):
        spec = _spec(
            x_axis={"column": "date", "aggregation": None},
            y_axis={"column": "revenue", "aggregation": "sum"},
            color_by="region",
        )
        sql = vizspec_to_sql(spec, "sales")
        assert '"region"' in sql
        assert 'GROUP BY "date", "region"' in sql

    def test_order_by_agg_column(self):
        spec = _spec(
            x_axis={"column": "region", "aggregation": None},
            y_axis={"column": "revenue", "aggregation": "sum"},
            sort_by={"column": "revenue", "direction": "desc"},
        )
        sql = vizspec_to_sql(spec, "sales")
        assert '"revenue_sum" DESC' in sql

    def test_limit(self):
        spec = _spec(limit=10)
        sql = vizspec_to_sql(spec, "sales")
        assert "LIMIT 10" in sql

    def test_full_spec_executes(self, sales_table):
        spec = _spec(
            intent="compare",
            x_axis={"column": "region", "aggregation": None},
            y_axis={"column": "revenue", "aggregation": "sum"},
            sort_by={"column": "revenue", "direction": "desc"},
            limit=5,
            chart_type="bar",
        )
        sql = vizspec_to_sql(spec, "test_sales")
        result = sales_table.execute(sql).fetchall()
        assert len(result) <= 5


class TestPrompt:
    def test_build_context_format(self):
        from lava.llm.prompt import build_context
        stats = {
            "region": {"type": "VARCHAR", "kind": "categorical", "unique_count": 5,
                       "null_count": 0, "row_count": 100, "top_values": ["NE", "W"]},
            "revenue": {"type": "DOUBLE", "kind": "numeric", "unique_count": 100,
                        "null_count": 0, "row_count": 100, "min": 10, "max": 5000, "mean": 500},
        }
        ctx = build_context(stats)
        assert "region" in ctx
        assert "VARCHAR" in ctx
        assert "numeric" in ctx

    def test_build_context_with_values(self):
        from lava.llm.prompt import build_context
        stats = {
            "region": {"type": "VARCHAR", "kind": "categorical", "unique_count": 5,
                       "null_count": 0, "row_count": 100, "top_values": ["Northeast", "West"]},
        }
        ctx = build_context(stats)
        assert "Northeast" in ctx

    def test_conversation_context(self):
        from lava.llm.conversation import ConversationContext
        ctx = ConversationContext()
        spec = _spec()
        ctx.add("show revenue", spec)
        ctx.add("by region", spec)
        history = ctx.build_history_prompt()
        assert "show revenue" in history
        assert "by region" in history
