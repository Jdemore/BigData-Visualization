"""Process-wide DuckDB connection. Tuned for analytical workloads: thread count
follows CPU count, memory limit is set to 70% of system RAM with spillover to
a temp directory so large queries don't OOM."""

import os
import tempfile

import duckdb
import psutil


def create_connection(
    db_path: str = ":memory:", threads: int | None = None
) -> duckdb.DuckDBPyConnection:
    """Create a tuned DuckDB connection. Call once at startup, reuse everywhere.

    Use a file-backed database (not :memory:) when you need query results
    or metadata to persist across application restarts.
    """
    con = duckdb.connect(db_path)

    cpu_count = os.cpu_count() or 4
    thread_count = threads or max(1, cpu_count - 1)
    con.execute(f"SET threads = {thread_count}")
    con.execute("SET enable_progress_bar = false")
    con.execute("SET enable_object_cache = true")

    # Memory: let DuckDB use up to 70% of system RAM, spill the rest to disk
    ram_gb = psutil.virtual_memory().total / (1024**3)
    mem_limit = f"{max(1, int(ram_gb * 0.7))}GB"
    con.execute(f"SET memory_limit = '{mem_limit}'")

    # Platform-aware temp directory
    swap_dir = os.path.join(tempfile.gettempdir(), "lava_duckdb_swap")
    con.execute(f"SET temp_directory = '{swap_dir}'")

    return con


# Module-level singleton
_con: duckdb.DuckDBPyConnection | None = None


def get_connection() -> duckdb.DuckDBPyConnection:
    """Return the singleton DuckDB connection, creating it on first call."""
    global _con
    if _con is None:
        _con = create_connection()
    return _con


def reset_connection() -> None:
    """Reset the singleton. Used by tests for isolation."""
    global _con
    if _con is not None:
        _con.close()
    _con = None
