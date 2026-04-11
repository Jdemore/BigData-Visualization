"""Metadata catalog — track every ingested dataset."""

import json

import duckdb


def init_catalog(con: duckdb.DuckDBPyConnection) -> None:
    """Create the catalog table if it doesn't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS __lava_catalog (
            name       VARCHAR PRIMARY KEY,
            path       VARCHAR NOT NULL,
            columns    VARCHAR NOT NULL,
            dtypes     VARCHAR NOT NULL,
            row_count  BIGINT,
            size_bytes BIGINT,
            indexed    BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def catalog_register(
    con: duckdb.DuckDBPyConnection, name: str, path: str, profile: dict
) -> None:
    """Register or update a dataset in the catalog."""
    con.execute(
        """
        INSERT OR REPLACE INTO __lava_catalog
            (name, path, columns, dtypes, row_count, size_bytes)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        [
            name,
            path,
            json.dumps(profile["columns"]),
            json.dumps(profile["dtypes"]),
            profile["estimated_rows"],
            profile["size_bytes"],
        ],
    )


def catalog_list(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Return all cataloged datasets."""
    rows = con.execute("SELECT * FROM __lava_catalog ORDER BY name").fetchall()
    cols = [
        "name",
        "path",
        "columns",
        "dtypes",
        "row_count",
        "size_bytes",
        "indexed",
        "created_at",
    ]
    return [dict(zip(cols, r)) for r in rows]
