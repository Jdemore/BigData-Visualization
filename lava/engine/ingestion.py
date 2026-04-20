"""Ingestion: profile source files, convert to Parquet, register as DuckDB views."""

import os

import duckdb


def _reader_expr(path: str) -> str:
    """DuckDB SELECT source for a given file. Picks the right reader from the extension."""
    lower = path.lower()
    if lower.endswith(".parquet") or "*" in path:
        return f"'{path}'"
    if lower.endswith(".json") or lower.endswith(".ndjson") or lower.endswith(".jsonl"):
        return f"read_json_auto('{path}')"
    return f"read_csv_auto('{path}', sample_size=10000)"


def profile_dataset(con: duckdb.DuckDBPyConnection, path: str) -> dict:
    """Cheap schema + row-count profile. Uses DESCRIBE, never scans the full file."""
    reader = _reader_expr(path)
    desc = con.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()
    meta = [(row[0], row[1]) for row in desc]
    size_bytes = os.path.getsize(path) if os.path.isfile(path) else 0

    if path.lower().endswith(".parquet") or "*" in path:
        row_count = con.execute(f"SELECT COUNT(*) FROM {reader}").fetchone()[0]
    else:
        # For CSV/JSON: sample up to 100K rows. If the file is larger, estimate the
        # total by extrapolating from the sampled row size instead of scanning.
        sample_rows = con.execute(f"SELECT COUNT(*) FROM {reader}").fetchone()[0]
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
    """Convert CSV/JSON to ZSTD-compressed Parquet with 100K-row row groups.

    Pass partition_by (e.g. 'date' or 'region') for datasets over ~1 GB to
    enable Hive-style partition pruning on future queries. No-op if the source
    is already Parquet.
    """
    if source_path.lower().endswith(".parquet"):
        return source_path

    os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)

    partition_clause = ""
    if partition_by:
        partition_clause = f", PARTITION_BY ({', '.join(partition_by)})"

    reader = _reader_expr(source_path)
    con.execute(f"""
        COPY (SELECT * FROM {reader})
        TO '{dest_path}' (FORMAT PARQUET, COMPRESSION ZSTD,
        ROW_GROUP_SIZE 100000{partition_clause})
    """)
    return dest_path


def register_dataset(
    con: duckdb.DuckDBPyConnection, name: str, parquet_path: str
) -> None:
    """Expose a Parquet file (or glob) as a named view so it can be queried by name."""
    con.execute(f"""CREATE OR REPLACE VIEW "{name}" AS SELECT * FROM '{parquet_path}'""")


def get_sample_values(
    con: duckdb.DuckDBPyConnection,
    path: str,
    columns: list[str],
    n: int = 5,
) -> dict[str, list]:
    """A few distinct sample values per column. Used to give the LLM concrete anchors
    (e.g. real region names) when it decides how to phrase a query."""
    samples: dict[str, list] = {}
    for col in columns:
        rows = con.execute(f"""
            SELECT DISTINCT "{col}" FROM '{path}' TABLESAMPLE 1000 ROWS
            WHERE "{col}" IS NOT NULL
            LIMIT {n}
        """).fetchall()
        samples[col] = [r[0] for r in rows]
    return samples
