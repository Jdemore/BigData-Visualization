"""Compile a validated VizSpec into DuckDB SQL.

This layer is deterministic -- no LLM involvement. Given the same VizSpec it
always produces the same SQL, which makes queries reproducible and auditable:
every chart shows its generating SQL in the app footer.
"""

import re

from lava.llm.schema import VizSpec

# Scans for any aggregate function call. Used to decide whether the query
# needs a GROUP BY when the LLM inlines aggregates into an expression instead
# of setting the aggregation field.
_AGG_RE = re.compile(
    r"\b(COUNT|SUM|AVG|MIN|MAX|MEDIAN|STDDEV|STDDEV_SAMP|STDDEV_POP)\s*\(",
    re.IGNORECASE,
)

# Matches AGG(DISTINCT expr) OVER (...) where expr may contain one nested call.
# DuckDB rejects DISTINCT inside window aggregates, so we rewrite to
# SUM(AGG(DISTINCT expr)) OVER (...) -- the standard percent-of-total pattern.
_DISTINCT_WINDOW_RE = re.compile(
    r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(\s*DISTINCT\s+"
    r"([^()]*(?:\([^()]*\)[^()]*)*)\)\s*(OVER\s*\()",
    re.IGNORECASE,
)


def _fix_distinct_window(expr: str) -> str:
    """Rewrite `AGG(DISTINCT x) OVER (...)` to `SUM(AGG(DISTINCT x)) OVER (...)`.

    The LLM likes to emit the first form even though the prompt tells it not to.
    Rather than relying on the model to get it right every time, we normalize
    here so the generated SQL is always valid DuckDB.
    """
    return _DISTINCT_WINDOW_RE.sub(r"SUM(\1(DISTINCT \2)) \3", expr)


def _agg_fn(func: str) -> str:
    """Map our short aggregation aliases to the DuckDB function names."""
    return {
        "std": "STDDEV_SAMP",
        "mean": "AVG",
    }.get(func, func.upper())


def _x_select(spec: VizSpec) -> str:
    """SELECT fragment for the x column. Applies DATE_TRUNC if a time_bucket is set."""
    col = spec.x_axis["column"]
    bucket = spec.x_axis.get("time_bucket")
    if bucket:
        return f"DATE_TRUNC('{bucket}', \"{col}\"::DATE) AS \"{col}\""
    return f'"{col}"'


def _x_group(spec: VizSpec) -> str:
    """GROUP BY fragment matching _x_select. Kept separate so the two stay in sync."""
    col = spec.x_axis["column"]
    bucket = spec.x_axis.get("time_bucket")
    if bucket:
        return f"DATE_TRUNC('{bucket}', \"{col}\"::DATE)"
    return f'"{col}"'


def _y_select(spec: VizSpec) -> str | None:
    """SELECT fragment for the y column (aggregation, expression, or raw)."""
    if not spec.y_axis:
        return None

    col = spec.y_axis["column"]
    agg = spec.y_axis.get("aggregation")
    expr = spec.y_axis.get("expression")
    label = spec.y_axis.get("label")

    value = expr if expr else f'"{col}"'
    if expr:
        value = _fix_distinct_window(value)

    if agg:
        sql_fn = _agg_fn(agg)
        alias = label or f"{col}_{agg}"
        return f'{sql_fn}({value}) AS "{alias}"'
    else:
        alias = label or col
        if expr:
            return f'({value}) AS "{alias}"'
        return f'"{col}"'


def vizspec_to_sql(spec: VizSpec, table_name: str) -> str:
    """Turn a VizSpec into a DuckDB query string.

    Order of clauses is SELECT -> WHERE -> GROUP BY -> ORDER BY -> LIMIT. The
    only subtlety is deciding whether we need GROUP BY: if the y-axis has an
    explicit aggregation OR its expression contains one inline, we must group.
    """
    y_agg = spec.y_axis.get("aggregation") if spec.y_axis else None
    y_expr = spec.y_axis.get("expression") if spec.y_axis else None
    # Catches percent-of-total and similar patterns where the LLM wrote the
    # aggregate directly into the expression and left aggregation=null.
    expr_has_agg = bool(y_expr and _AGG_RE.search(y_expr))
    needs_group_by = y_agg is not None or expr_has_agg

    select_parts: list[str] = [_x_select(spec)]

    y_expr = _y_select(spec)
    if y_expr:
        select_parts.append(y_expr)

    if spec.color_by:
        select_parts.append(f'"{spec.color_by}"')

    sql = f'SELECT {", ".join(select_parts)} FROM "{table_name}"'

    if spec.filters:
        conditions: list[str] = []
        for f in spec.filters:
            col, op, val = f["column"], f["op"], f["value"]
            if op == "contains":
                # CAST covers TIMESTAMP/numeric columns where ILIKE would otherwise fail.
                conditions.append(f'CAST("{col}" AS VARCHAR) ILIKE \'%{val}%\'')
            elif op == "not_contains":
                conditions.append(f'CAST("{col}" AS VARCHAR) NOT ILIKE \'%{val}%\'')
            elif isinstance(val, bool):
                # Python True/False would render as 'True'; DuckDB needs lowercase.
                conditions.append(f'"{col}" {op} {str(val).lower()}')
            elif isinstance(val, str):
                conditions.append(f'"{col}" {op} \'{val}\'')
            else:
                conditions.append(f'"{col}" {op} {val}')
        sql += " WHERE " + " AND ".join(conditions)

    if needs_group_by:
        group_parts = [_x_group(spec)]
        if spec.color_by:
            group_parts.append(f'"{spec.color_by}"')
        sql += " GROUP BY " + ", ".join(group_parts)

    # sort_by.column refers to a VizSpec column name, which may or may not be
    # the actual SQL output alias. Translate before emitting ORDER BY.
    x_alias = spec.x_axis["column"]
    y_alias = (spec.y_axis.get("label") or f"{spec.y_axis['column']}_{y_agg}") if spec.y_axis and y_agg else None
    output_aliases = {x_alias, y_alias, spec.color_by} - {None}

    if spec.sort_by and spec.sort_by.get("column"):
        direction = spec.sort_by.get("direction", "asc").upper()
        sort_col = spec.sort_by["column"]
        if sort_col not in output_aliases:
            if y_alias and spec.y_axis and sort_col == spec.y_axis["column"]:
                sort_col = y_alias
            elif sort_col not in output_aliases:
                # Fallback: sort by x rather than error out on an unknown column.
                sort_col = x_alias
        sql += f' ORDER BY "{sort_col}" {direction}'
    elif needs_group_by:
        sql += f' ORDER BY "{x_alias}"'

    if spec.limit:
        sql += f" LIMIT {spec.limit}"

    return sql
