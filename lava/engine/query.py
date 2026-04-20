"""SQL in, Arrow out. Every query returns through here so timing and row caps
are consistent, and the renderer only deals with one data shape."""

import time
from dataclasses import dataclass, field

import duckdb
import pyarrow as pa


@dataclass
class DataResult:
    """Query output plus the metadata the UI surfaces (timing, warnings, the SQL)."""

    arrow_table: pa.Table
    row_count: int
    query_ms: float
    sql: str
    warnings: list[str] = field(default_factory=list)

    def to_pandas(self):
        """Only call this at the Plotly/Dash boundary. Keep Arrow everywhere else
        to preserve zero-copy handoff and avoid eager materialization."""
        return self.arrow_table.to_pandas()


def execute_query(
    con: duckdb.DuckDBPyConnection, sql: str, max_viz_rows: int = 100_000
) -> DataResult:
    """Run SQL against DuckDB and return an Arrow-backed DataResult.

    Applies a LIMIT of max_viz_rows + 1 as a safety cap; if the cap triggers,
    a second COUNT(*) fills in the true size for the warning message so the user
    knows their chart is showing a truncated view.
    """
    warnings: list[str] = []
    t0 = time.perf_counter()

    # Fetch one extra row so we can detect truncation without a separate COUNT.
    limited_sql = f"SELECT * FROM ({sql}) AS __q LIMIT {max_viz_rows + 1}"
    arrow_table = con.execute(limited_sql).fetch_arrow_table()

    if arrow_table.num_rows > max_viz_rows:
        arrow_table = arrow_table.slice(0, max_viz_rows)
        try:
            true_count = con.execute(
                f"SELECT COUNT(*) FROM ({sql}) AS __q"
            ).fetchone()[0]
        except Exception:
            true_count = max_viz_rows
        warnings.append(
            f"Result has {true_count:,} rows, truncated to {max_viz_rows:,} for visualization"
        )

    elapsed_ms = (time.perf_counter() - t0) * 1000

    return DataResult(
        arrow_table=arrow_table,
        row_count=arrow_table.num_rows,
        query_ms=elapsed_ms,
        sql=sql,
        warnings=warnings,
    )
