"""VizSpec to DuckDB SQL — deterministic, no LLM involvement."""

from lava.llm.schema import VizSpec


def _agg_fn(func: str) -> str:
    """Map aggregation name to SQL function."""
    return {
        "std": "STDDEV_SAMP",
        "mean": "AVG",
    }.get(func, func.upper())


def _x_select(spec: VizSpec) -> str:
    """Build the x-axis SELECT expression."""
    col = spec.x_axis["column"]
    bucket = spec.x_axis.get("time_bucket")
    if bucket:
        return f"DATE_TRUNC('{bucket}', \"{col}\"::DATE) AS \"{col}\""
    return f'"{col}"'


def _x_group(spec: VizSpec) -> str:
    """Build the x-axis GROUP BY expression (matches SELECT)."""
    col = spec.x_axis["column"]
    bucket = spec.x_axis.get("time_bucket")
    if bucket:
        return f"DATE_TRUNC('{bucket}', \"{col}\"::DATE)"
    return f'"{col}"'


def _y_select(spec: VizSpec) -> str | None:
    """Build the y-axis SELECT expression."""
    if not spec.y_axis:
        return None

    col = spec.y_axis["column"]
    agg = spec.y_axis.get("aggregation")
    expr = spec.y_axis.get("expression")
    label = spec.y_axis.get("label")

    # Base value: expression or plain column
    value = expr if expr else f'"{col}"'

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
    """Convert a validated VizSpec into a DuckDB SQL query."""
    y_agg = spec.y_axis.get("aggregation") if spec.y_axis else None
    needs_group_by = y_agg is not None

    # SELECT
    select_parts: list[str] = [_x_select(spec)]

    y_expr = _y_select(spec)
    if y_expr:
        select_parts.append(y_expr)

    if spec.color_by:
        select_parts.append(f'"{spec.color_by}"')

    sql = f'SELECT {", ".join(select_parts)} FROM "{table_name}"'

    # WHERE
    if spec.filters:
        conditions: list[str] = []
        for f in spec.filters:
            col, op, val = f["column"], f["op"], f["value"]
            if op == "contains":
                conditions.append(f'"{col}" ILIKE \'%{val}%\'')
            elif op == "not_contains":
                conditions.append(f'"{col}" NOT ILIKE \'%{val}%\'')
            elif isinstance(val, str):
                conditions.append(f'"{col}" {op} \'{val}\'')
            else:
                conditions.append(f'"{col}" {op} {val}')
        sql += " WHERE " + " AND ".join(conditions)

    # GROUP BY
    if needs_group_by:
        group_parts = [_x_group(spec)]
        if spec.color_by:
            group_parts.append(f'"{spec.color_by}"')
        sql += " GROUP BY " + ", ".join(group_parts)

    # ORDER BY — resolve against actual output column names
    x_alias = spec.x_axis["column"]
    y_alias = (spec.y_axis.get("label") or f"{spec.y_axis['column']}_{y_agg}") if spec.y_axis and y_agg else None
    output_aliases = {x_alias, y_alias, spec.color_by} - {None}

    if spec.sort_by and spec.sort_by.get("column"):
        direction = spec.sort_by.get("direction", "asc").upper()
        sort_col = spec.sort_by["column"]
        # Map to actual output alias
        if sort_col not in output_aliases:
            if y_alias and spec.y_axis and sort_col == spec.y_axis["column"]:
                sort_col = y_alias
            elif sort_col not in output_aliases:
                sort_col = x_alias  # safe fallback: sort by x
        sql += f' ORDER BY "{sort_col}" {direction}'
    elif needs_group_by:
        sql += f' ORDER BY "{x_alias}"'

    # LIMIT
    if spec.limit:
        sql += f" LIMIT {spec.limit}"

    return sql
