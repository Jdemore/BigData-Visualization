"""Single entry point for ingesting a dataset. Runs profile -> Parquet
conversion -> view registration -> catalog insert -> stats extraction in order,
and returns everything the UI needs to start answering queries."""

import os

import duckdb

from lava.engine.catalog import catalog_register, init_catalog
from lava.engine.ingestion import (
    ensure_parquet,
    profile_dataset,
    register_dataset,
)
from lava.engine.stats import extract_column_stats


def bootstrap(
    con: duckdb.DuckDBPyConnection,
    data_path: str,
    name: str | None = None,
) -> tuple[str, dict[str, dict]]:
    """Full ingestion pipeline. Returns (table_name, column_stats).

    column_stats contains everything the LLM needs: types, ranges,
    cardinality, sample values, date spans.
    """
    if name is None:
        name = os.path.splitext(os.path.basename(data_path))[0]

    profile = profile_dataset(con, data_path)

    parquet_dir = os.path.join(os.path.dirname(data_path), ".parquet_cache")
    parquet_path = os.path.join(parquet_dir, f"{name}.parquet")
    parquet_path = ensure_parquet(con, data_path, parquet_path)

    register_dataset(con, name, parquet_path)

    init_catalog(con)
    catalog_register(con, name, parquet_path, profile)

    # Extract rich column stats for the LLM
    column_stats = extract_column_stats(con, name)

    return name, column_stats
