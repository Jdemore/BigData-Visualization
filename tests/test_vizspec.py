"""Tests for VizSpec parsing and validation."""

from lava.llm.parser import _infer_chart, parse_llm_response
from lava.llm.schema import VizSpec

SCHEMA = {
    "order_id": "INTEGER",
    "region": "VARCHAR",
    "revenue": "DOUBLE",
    "quantity": "INTEGER",
}


class TestVizSpec:
    def test_dataclass_fields(self):
        spec = VizSpec(
            intent="explore",
            x_axis={"column": "region", "aggregation": None},
            y_axis=None, color_by=None, filters=None,
            sort_by=None, limit=None, chart_type="table", title="Test",
        )
        assert spec.intent == "explore"
        assert spec.chart_type == "table"
        assert spec.chart_suggestion == "table"  # compat property


class TestParseResponse:
    def test_valid_response(self):
        data = {
            "intent": "compare",
            "x_axis": {"column": "region"},
            "y_axis": {"column": "revenue", "aggregation": "sum"},
            "chart_type": "bar",
            "title": "Revenue by Region",
        }
        spec = parse_llm_response(data, SCHEMA)
        assert spec.intent == "compare"
        assert spec.x_axis["column"] == "region"
        assert spec.y_axis["column"] == "revenue"
        assert spec.y_axis["aggregation"] == "sum"
        assert spec.chart_type == "bar"

    def test_invalid_intent_defaults(self):
        data = {
            "intent": "banana",
            "x_axis": {"column": "region"},
            "chart_type": "bar", "title": "X",
        }
        spec = parse_llm_response(data, SCHEMA)
        assert spec.intent == "explore"

    def test_column_autocorrect(self):
        data = {
            "intent": "explore",
            "x_axis": {"column": "revenu"},
            "chart_type": "table", "title": "X",
        }
        spec = parse_llm_response(data, SCHEMA)
        assert spec.x_axis["column"] == "revenue"

    def test_no_x_axis_fallback(self):
        data = {"intent": "explore", "chart_type": "table", "title": "X"}
        spec = parse_llm_response(data, SCHEMA)
        assert spec.x_axis["column"] in SCHEMA

    def test_filter_validates_op(self):
        data = {
            "intent": "explore",
            "x_axis": {"column": "revenue"},
            "filters": [{"column": "revenue", "op": "BOGUS", "value": 10}],
            "chart_type": "table", "title": "X",
        }
        spec = parse_llm_response(data, SCHEMA)
        assert spec.filters is None

    def test_aggregation_validates_func(self):
        data = {
            "intent": "compare",
            "x_axis": {"column": "region"},
            "y_axis": {"column": "revenue", "aggregation": "BOGUS"},
            "chart_type": "bar", "title": "X",
        }
        spec = parse_llm_response(data, SCHEMA)
        assert spec.y_axis["aggregation"] is None

    def test_chart_fallback(self):
        data = {
            "intent": "trend",
            "x_axis": {"column": "revenue"},
            "chart_type": "INVALID", "title": "X",
        }
        spec = parse_llm_response(data, SCHEMA)
        assert spec.chart_type == "line"

    def test_color_by(self):
        data = {
            "intent": "compare",
            "x_axis": {"column": "region"},
            "y_axis": {"column": "revenue", "aggregation": "sum"},
            "color_by": "region",
            "chart_type": "bar", "title": "X",
        }
        spec = parse_llm_response(data, SCHEMA)
        assert spec.color_by == "region"


class TestInferChart:
    def test_mapping(self):
        assert _infer_chart("trend") == "line"
        assert _infer_chart("compare") == "bar"
        assert _infer_chart("distribution") == "histogram"
        assert _infer_chart("correlation") == "scatter"
        assert _infer_chart("composition") == "pie"
        assert _infer_chart("explore") == "table"
