"""SQL-based data reduction for large datasets."""

import duckdb

from lava.engine.query import DataResult
from lava.llm.schema import VizSpec


def reduce_data(spec: VizSpec, data_result: DataResult, con: duckdb.DuckDBPyConnection):
    """Reduce a large DataResult to viz-ready pandas. Picks strategy by chart type."""
    sql = data_result.sql
    chart = spec.chart_type

    if chart == "histogram":
        reduced_sql = _bin_sql(sql, spec)
    elif chart in ("bar", "line") and spec.color_by:
        reduced_sql = _aggregate_sql(sql, spec)
    elif chart == "line":
        reduced_sql = _lttb_sql(sql, target=5000)
    else:
        reduced_sql = f"SELECT * FROM ({sql}) AS __q USING SAMPLE 5000 ROWS (reservoir)"

    return con.execute(reduced_sql).fetch_arrow_table().to_pandas()


def _bin_sql(source_sql: str, spec: VizSpec, n_bins: int = 100) -> str:
    """Server-side histogram binning via DuckDB FLOOR-based bucketing."""
    col = spec.x_axis["column"]
    return f"""
        WITH bounds AS (
            SELECT MIN("{col}") AS mn, MAX("{col}") AS mx FROM ({source_sql}) AS __q
        ),
        binned AS (
            SELECT
                CASE
                    WHEN bounds.mx = bounds.mn THEN 0
                    ELSE LEAST(FLOOR(("{col}" - bounds.mn) / ((bounds.mx - bounds.mn) / {n_bins})), {n_bins} - 1)
                END AS bin_idx,
                bounds.mn, bounds.mx
            FROM ({source_sql}) AS __q, bounds
        )
        SELECT
            mn + bin_idx * ((mx - mn) / {n_bins}) AS bin_start,
            COUNT(*) AS count
        FROM binned
        GROUP BY bin_start, mn, mx, bin_idx
        ORDER BY bin_start
    """


def _aggregate_sql(source_sql: str, spec: VizSpec) -> str:
    """Group-by aggregation for bar/line charts."""
    group_parts = [f'"{spec.x_axis["column"]}"']
    if spec.color_by:
        group_parts.append(f'"{spec.color_by}"')
    group_cols = ", ".join(group_parts)

    if spec.y_axis and spec.y_axis.get("aggregation"):
        from lava.llm.sql_gen import _agg_fn
        col = spec.y_axis["column"]
        func = spec.y_axis["aggregation"]
        agg_clause = f'{_agg_fn(func)}("{col}") AS "{col}_{func}"'
    else:
        agg_clause = "COUNT(*) AS count"
    return f"""
        SELECT {group_cols}, {agg_clause}
        FROM ({source_sql}) AS __q
        GROUP BY {group_cols} ORDER BY {group_cols}
    """


def _lttb_sql(source_sql: str, target: int = 5000) -> str:
    """LTTB downsampling via DuckDB window functions."""
    return f"""
        WITH numbered AS (
            SELECT *, row_number() OVER () AS __rn,
                   COUNT(*) OVER () AS __total
            FROM ({source_sql}) AS __src
        ),
        bucketed AS (
            SELECT *,
                   CASE
                       WHEN __rn = 1 THEN 0
                       WHEN __rn = __total THEN {target} - 1
                       ELSE 1 + CAST((__rn - 2) * ({target} - 2.0) / (__total - 2) AS INTEGER)
                   END AS __bucket
            FROM numbered
        )
        SELECT * EXCLUDE (__rn, __total, __bucket) FROM (
            SELECT *, row_number() OVER (PARTITION BY __bucket ORDER BY __rn) AS __pick
            FROM bucketed
        ) WHERE __pick = 1
    """
