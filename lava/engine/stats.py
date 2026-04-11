"""Column statistics extraction — gives the LLM context to make smart decisions."""

import duckdb


def extract_column_stats(
    con: duckdb.DuckDBPyConnection, table_name: str
) -> dict[str, dict]:
    """Extract per-column statistics for LLM context injection.

    Uses batched queries to minimize round-trips: one query for all
    unique/null counts, one for all numeric stats, then per-column
    only for categorical top-values and datetime ranges.
    """
    desc = con.execute(f'DESCRIBE SELECT * FROM "{table_name}"').fetchall()
    columns = [(row[0], row[1]) for row in desc]
    row_count = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]

    # Classify all columns
    col_kinds = {name: _classify_type(dtype) for name, dtype in columns}

    # Batch 1: unique + null counts for ALL columns in a single query
    count_exprs = []
    for name, _ in columns:
        count_exprs.append(f'COUNT(DISTINCT "{name}") AS "uniq_{name}"')
        count_exprs.append(
            f'SUM(CASE WHEN "{name}" IS NULL THEN 1 ELSE 0 END) AS "null_{name}"'
        )
    count_row = con.execute(
        f"SELECT {', '.join(count_exprs)} FROM \"{table_name}\""
    ).fetchone()

    # Batch 2: min/max/avg for ALL numeric columns in a single query
    numeric_cols = [n for n, k in col_kinds.items() if k == "numeric"]
    numeric_stats: dict[str, tuple] = {}
    if numeric_cols:
        num_exprs = []
        for name in numeric_cols:
            num_exprs.append(f'MIN("{name}")')
            num_exprs.append(f'MAX("{name}")')
            num_exprs.append(f'AVG("{name}")::DOUBLE')
        num_row = con.execute(
            f"SELECT {', '.join(num_exprs)} FROM \"{table_name}\""
        ).fetchone()
        for i, name in enumerate(numeric_cols):
            numeric_stats[name] = (num_row[i * 3], num_row[i * 3 + 1], num_row[i * 3 + 2])

    # Build stats dict
    stats: dict[str, dict] = {}
    for idx, (name, dtype) in enumerate(columns):
        kind = col_kinds[name]
        info: dict = {
            "type": dtype,
            "kind": kind,
            "row_count": row_count,
            "unique_count": count_row[idx * 2],
            "null_count": count_row[idx * 2 + 1],
        }

        if kind == "numeric" and name in numeric_stats:
            mn, mx, avg = numeric_stats[name]
            info["min"] = mn
            info["max"] = mx
            info["mean"] = round(avg, 2) if avg is not None else None

        elif kind == "datetime":
            dates = con.execute(f"""
                SELECT MIN("{name}")::VARCHAR, MAX("{name}")::VARCHAR,
                       DATEDIFF('day', MIN("{name}")::DATE, MAX("{name}")::DATE)
                FROM "{table_name}"
            """).fetchone()
            info["min_date"] = dates[0]
            info["max_date"] = dates[1]
            info["span_days"] = dates[2]

        elif kind == "categorical":
            top = con.execute(f"""
                SELECT "{name}", COUNT(*) AS cnt
                FROM "{table_name}"
                WHERE "{name}" IS NOT NULL
                GROUP BY "{name}"
                ORDER BY cnt DESC
                LIMIT 10
            """).fetchall()
            info["top_values"] = [str(row[0]) for row in top]

        stats[name] = info

    return stats


def _classify_type(sql_type: str) -> str:
    """Classify a SQL type into a semantic kind."""
    t = sql_type.upper()
    if any(x in t for x in ["INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC",
                              "REAL", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT"]):
        return "numeric"
    if any(x in t for x in ["DATE", "TIME", "TIMESTAMP", "INTERVAL"]):
        return "datetime"
    if "BOOL" in t:
        return "boolean"
    return "categorical"
