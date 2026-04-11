"""Data ingestion — profile, convert, register datasets."""

import os

import duckdb


def profile_dataset(con: duckdb.DuckDBPyConnection, path: str) -> dict:
    """Profile a dataset without fully loading it. Works on CSV, Parquet, and JSON."""
    if path.endswith(".parquet") or "*" in path:
        # Use DESCRIBE to get column info from Parquet
        desc = con.execute(f"DESCRIBE SELECT * FROM '{path}'").fetchall()
        meta = [(row[0], row[1]) for row in desc]
        row_count = con.execute(f"""
            SELECT COUNT(*) FROM '{path}'
        """).fetchone()[0]
        size_bytes = os.path.getsize(path) if os.path.isfile(path) else 0
    else:
        desc = con.execute(f"""
            DESCRIBE SELECT * FROM read_csv_auto('{path}', sample_size=10000)
        """).fetchall()
        meta = [(row[0], row[1]) for row in desc]
        size_bytes = os.path.getsize(path)
        sample_rows = con.execute(f"""
            SELECT COUNT(*) FROM read_csv_auto('{path}', sample_size=10000)
        """).fetchone()[0]
        avg_row_bytes = size_bytes / max(sample_rows, 1)
        row_count = (
            int(size_bytes / avg_row_bytes) if sample_rows >= 100000 else sample_rows
        )

    schema = {col: dtype for col, dtype in meta}
    return {
        "columns": list(schema.keys()),
        "dtypes": schema,
        "estimated_rows": row_count,
        "size_bytes": size_bytes,
    }


def ensure_parquet(
    con: duckdb.DuckDBPyConnection,
    source_path: str,
    dest_path: str,
    partition_by: list[str] | None = None,
) -> str:
    """Convert any supported format to Parquet. Returns output path.

    For datasets > 1GB, pass partition_by with a column like 'date' or 'region'
    to enable Hive partition pruning on future queries.
    """
    if source_path.endswith(".parquet"):
        return source_path

    os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)

    partition_clause = ""
    if partition_by:
        partition_clause = f", PARTITION_BY ({', '.join(partition_by)})"

    con.execute(f"""
        COPY (SELECT * FROM read_csv_auto('{source_path}'))
        TO '{dest_path}' (FORMAT PARQUET, COMPRESSION ZSTD,
        ROW_GROUP_SIZE 100000{partition_clause})
    """)
    return dest_path


def register_dataset(
    con: duckdb.DuckDBPyConnection, name: str, parquet_path: str
) -> None:
    """Register a Parquet file (or glob) as a named DuckDB view."""
    con.execute(f"""CREATE OR REPLACE VIEW "{name}" AS SELECT * FROM '{parquet_path}'""")


def get_sample_values(
    con: duckdb.DuckDBPyConnection,
    path: str,
    columns: list[str],
    n: int = 5,
) -> dict[str, list]:
    """Fetch a few distinct sample values per column for LLM context injection."""
    samples: dict[str, list] = {}
    for col in columns:
        rows = con.execute(f"""
            SELECT DISTINCT "{col}" FROM '{path}' TABLESAMPLE 1000 ROWS
            WHERE "{col}" IS NOT NULL
            LIMIT {n}
        """).fetchall()
        samples[col] = [r[0] for r in rows]
    return samples
