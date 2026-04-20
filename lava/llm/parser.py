"""Translate raw LLM JSON into a validated VizSpec.

This layer is deliberately defensive: the LLM occasionally hallucinates column
names, emits dicts where strings are expected, or uses the wrong equality
operator. Rather than surface those as hard errors, we fuzz-match column names,
coerce types, and quietly drop filters with unrecognized ops. The raw response
is logged upstream, so nothing is lost silently."""

import difflib
import re

from lava.llm.schema import (
    VALID_AGGS,
    VALID_CHARTS,
    VALID_INTENTS,
    VALID_OPS,
    VALID_TIME_BUCKETS,
    VizSpec,
)


def _resolve_column(col: str, available: set[str]) -> str | None:
    """Exact match first, then a difflib near-match at 0.7 ratio. The model occasionally
    emits 'product_cat' for 'product_category' or similar -- fuzzy match rescues those."""
    if col in available:
        return col
    matches = difflib.get_close_matches(col, available, n=1, cutoff=0.7)
    return matches[0] if matches else None


def _validate_expression(expr: str | None, available: set[str]) -> str | None:
    """Reject expressions that contain anything outside a safe grammar of column
    names, numbers, arithmetic, and whitelisted SQL functions. Prevents a
    hallucinating LLM from smuggling arbitrary SQL through the expression slot."""
    if not expr:
        return None
    safe_pattern = re.compile(
        r'^[\w\s\.\,\+\-\*\/\(\)\"\'0-9]+'
        r'(?:\s*(?:OVER|PARTITION|BY|ORDER|NULLIF|COALESCE|CAST|AS|SUM|AVG|COUNT|MIN|MAX)\s*[\(\)]?)*'
        r'[\w\s\.\,\+\-\*\/\(\)\"\'0-9]*$',
        re.IGNORECASE,
    )
    if not safe_pattern.match(expr):
        return None
    return expr


def _parse_axis(data: dict | None, available: set[str]) -> dict | None:
    """Parse an axis spec dict, validating all fields."""
    if not data:
        return None
    col = _resolve_column(data.get("column", ""), available)
    if not col:
        return None

    agg = data.get("aggregation")
    if agg and agg not in VALID_AGGS:
        agg = None

    time_bucket = data.get("time_bucket")
    if time_bucket and time_bucket not in VALID_TIME_BUCKETS:
        time_bucket = None

    expression = _validate_expression(data.get("expression"), available)
    label = data.get("label")

    return {
        "column": col,
        "aggregation": agg,
        "time_bucket": time_bucket,
        "expression": expression,
        "label": label,
    }


def parse_llm_response(data: dict, schema: dict) -> VizSpec:
    """Build a VizSpec from raw LLM JSON. Defensive against malformed fields."""
    available = set(schema.keys())

    intent = data.get("intent", "explore")
    if intent not in VALID_INTENTS:
        intent = "explore"

    x_axis = _parse_axis(data.get("x_axis"), available)
    y_axis = _parse_axis(data.get("y_axis"), available)

    if not x_axis:
        # Missing x_axis would break every downstream caller. Fall back to the
        # first column so the user at least sees a table instead of a stack trace.
        first_col = list(available)[0]
        x_axis = {"column": first_col, "aggregation": None, "time_bucket": None,
                   "expression": None, "label": None}

    # color_by has been observed as a string, a dict ({"column": "x"}), or a list.
    # Normalize to a single column name regardless of shape.
    color_by = data.get("color_by")
    if isinstance(color_by, dict):
        color_by = color_by.get("column") or color_by.get("name")
    if isinstance(color_by, list):
        color_by = color_by[0] if color_by else None
    if isinstance(color_by, str):
        color_by = _resolve_column(color_by, available)
    else:
        color_by = None

    filters: list[dict] = []
    for f in data.get("filters", []) or []:
        col = _resolve_column(f.get("column", ""), available)
        op = f.get("op")
        # The LLM sometimes writes Python-style "==" instead of SQL "=".
        if op == "==":
            op = "="
        if col and op in VALID_OPS:
            filters.append({"column": col, "op": op, "value": f["value"]})

    chart = data.get("chart_type") or data.get("chart_suggestion", "table")
    if not isinstance(chart, str) or chart not in VALID_CHARTS:
        chart = _infer_chart(intent)

    sort_by = data.get("sort_by")
    if isinstance(sort_by, dict):
        sort_col = sort_by.get("column")
        if not isinstance(sort_col, str):
            sort_by = None
    else:
        sort_by = None

    limit = data.get("limit")
    if not isinstance(limit, int):
        limit = None

    return VizSpec(
        intent=intent,
        x_axis=x_axis,
        y_axis=y_axis,
        color_by=color_by,
        filters=filters if filters else None,
        sort_by=sort_by,
        limit=limit,
        chart_type=chart,
        title=str(data.get("title", "Data Visualization")),
    )


def _infer_chart(intent: str) -> str:
    """Pick a reasonable default chart when the LLM's suggestion is invalid or missing."""
    return {
        "trend": "line",
        "compare": "bar",
        "distribution": "histogram",
        "correlation": "scatter",
        "composition": "pie",
        "explore": "table",
    }.get(intent, "table")
