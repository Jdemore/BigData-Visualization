"""Index builders — DuckDB sort/extract then delegate to index classes."""

import os

import duckdb

from lava.index.bptree import BPlusTree
from lava.index.gridfile import GridFile


def build_bptree_index(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    column: str,
    index_dir: str,
) -> BPlusTree:
    """Sort column via DuckDB, bulk-load into B+-tree on disk."""
    os.makedirs(index_dir, exist_ok=True)

    arrow = con.execute(f"""
        SELECT "{column}", row_number() OVER (ORDER BY rowid) - 1 AS __rowid__
        FROM "{table_name}"
        ORDER BY "{column}"
    """).fetch_arrow_table()

    tree = BPlusTree(path=os.path.join(index_dir, f"{column}.bpt"))
    tree.build_from_sorted(
        arrow.column(column).to_numpy(),
        arrow.column("__rowid__").to_numpy(),
    )
    return tree


def build_gridfile_index(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    dims: list[str],
    index_dir: str,
    target_bucket_size: int = 10000,
) -> GridFile:
    """Build a grid file index on the given dimensions."""
    os.makedirs(index_dir, exist_ok=True)

    grid = GridFile(bucket_dir=index_dir)
    grid.build(con, table_name, dims, target_bucket_size)
    return grid
