"""Tests for LLM pipeline — mocked Gemini calls."""

import json
from unittest.mock import patch

from lava.llm.pipeline import _vizspec_cache, nl_to_vizspec
from lava.llm.schema import VizSpec

COLUMN_STATS = {
    "region": {"type": "VARCHAR", "kind": "categorical", "unique_count": 5,
               "null_count": 0, "row_count": 100, "top_values": ["Northeast", "West"]},
    "revenue": {"type": "DOUBLE", "kind": "numeric", "unique_count": 100,
                "null_count": 0, "row_count": 100, "min": 10, "max": 5000, "mean": 500},
    "quantity": {"type": "INTEGER", "kind": "numeric", "unique_count": 20,
                 "null_count": 0, "row_count": 100, "min": 1, "max": 20, "mean": 10},
}
SCHEMA = {"region": "VARCHAR", "revenue": "DOUBLE", "quantity": "INTEGER"}

VALID_RESPONSE = {
    "intent": "compare",
    "x_axis": {"column": "region", "aggregation": None},
    "y_axis": {"column": "revenue", "aggregation": "sum"},
    "color_by": None,
    "filters": None,
    "sort_by": None,
    "limit": None,
    "chart_type": "bar",
    "title": "Revenue by Region",
}


class TestPipeline:
    def setup_method(self):
        _vizspec_cache.clear()

    @patch("lava.llm.pipeline.query_llm")
    def test_cache_hit(self, mock_llm):
        mock_llm.return_value = VALID_RESPONSE
        spec1 = nl_to_vizspec("show revenue by region", COLUMN_STATS)
        spec2 = nl_to_vizspec("show revenue by region", COLUMN_STATS)
        assert mock_llm.call_count == 2  # step1 refine + step2 generate; second query hits cache
        assert spec1.intent == spec2.intent

    @patch("lava.llm.pipeline.query_llm")
    def test_retry_on_parse_error(self, mock_llm):
        mock_llm.side_effect = [
            json.JSONDecodeError("bad", "", 0),
            VALID_RESPONSE,
        ]
        spec = nl_to_vizspec("show revenue", COLUMN_STATS, use_cache=False)
        assert spec.intent == "compare"
        assert mock_llm.call_count == 2

    @patch("lava.llm.pipeline.query_llm")
    def test_fallback_on_total_failure(self, mock_llm):
        mock_llm.side_effect = ValueError("always fails")
        spec = nl_to_vizspec("show revenue", COLUMN_STATS, use_cache=False)
        assert spec.intent == "explore"
        assert spec.chart_suggestion == "table"
        assert spec.limit == 1000


class TestEval:
    def test_parse_success_rate(self):
        from lava.llm.eval import calc_parse_success_rate
        results = [
            {"success": True, "attempts": 1},
            {"success": True, "attempts": 2},
            {"success": False, "attempts": 3},
        ]
        rate = calc_parse_success_rate(results)
        assert abs(rate - 1 / 3) < 0.01

    def test_intent_accuracy(self):
        from lava.llm.eval import calc_intent_accuracy
        preds = ["compare", "trend", "explore"]
        labels = ["compare", "distribution", "explore"]
        acc = calc_intent_accuracy(preds, labels)
        assert abs(acc - 2 / 3) < 0.01

    def test_chart_score(self):
        from lava.llm.eval import score_chart_choice
        spec = VizSpec(
            intent="compare",
            x_axis={"column": "region", "aggregation": None},
            y_axis={"column": "revenue", "aggregation": "sum"},
            color_by=None, filters=None,
            sort_by=None, limit=None, chart_type="bar", title="X",
        )
        profile = {"n_unique_groups": 5, "has_datetime_column": False, "n_numeric_columns": 1}
        score = score_chart_choice(spec, profile)
        assert score == 1.0
