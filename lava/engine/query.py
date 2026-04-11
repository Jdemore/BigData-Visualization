"""Query execution — SQL in, Arrow out, with timing and safety limits."""

import time
from dataclasses import dataclass, field

import duckdb
import pyarrow as pa


@dataclass
class DataResult:
    """Result of a query execution. Arrow table + metadata."""

    arrow_table: pa.Table
    row_count: int
    query_ms: float
    sql: str
    warnings: list[str] = field(default_factory=list)

    def to_pandas(self):
        """Convert to pandas at the viz boundary only. Never for intermediate processing."""
        return self.arrow_table.to_pandas()


def execute_query(
    con: duckdb.DuckDBPyConnection, sql: str, max_viz_rows: int = 100_000
) -> DataResult:
    """Execute SQL, return result as Arrow with timing metadata.

    Fetches data directly with a safety LIMIT. If the limit triggers,
    runs a COUNT to report the true size in the warning.
    """
    warnings: list[str] = []
    t0 = time.perf_counter()

    # Always apply a safety limit to avoid unbounded results
    limited_sql = f"SELECT * FROM ({sql}) AS __q LIMIT {max_viz_rows + 1}"
    arrow_table = con.execute(limited_sql).fetch_arrow_table()

    if arrow_table.num_rows > max_viz_rows:
        # Truncated — get the true count for the warning message
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
