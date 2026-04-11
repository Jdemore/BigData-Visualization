"""Grid file index — multi-dimensional range queries with quantile-based splits."""

import os
from dataclasses import dataclass, field

import duckdb
import numpy as np


@dataclass
class GridFile:
    """Multi-dimensional grid file for range queries."""

    dimensions: list[str] = field(default_factory=list)
    scales: list[list[float]] = field(default_factory=list)
    bucket_dir: str = ""
    _row_id_buckets: dict[tuple, np.ndarray] = field(
        default_factory=dict, repr=False
    )

    def build(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        dims: list[str],
        target_bucket_size: int = 10000,
    ) -> None:
        """Build grid file. Uses DuckDB for quantile computation."""
        self.dimensions = dims
        self.scales = []

        n_rows = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]

        for dim in dims:
            n_splits = max(1, n_rows // target_bucket_size)
            quantiles = [i / n_splits for i in range(1, n_splits)]
            q_values: list[float] = []
            for q in quantiles:
                val = con.execute(f"""
                    SELECT quantile_cont("{dim}", {q}) FROM "{table_name}"
                """).fetchone()[0]
                q_values.append(float(val))
            # Remove duplicates and sort
            q_values = sorted(set(q_values))
            self.scales.append(q_values)

        os.makedirs(self.bucket_dir, exist_ok=True)

        # Assign each row to a cell and store row IDs per cell
        self._row_id_buckets = {}

        # Build cell assignment query
        select_parts = ['rowid AS __rid']
        for i, dim in enumerate(dims):
            scale = self.scales[i]
            if not scale:
                select_parts.append(f"0 AS __cell_{i}")
            else:
                case_parts = []
                for j, threshold in enumerate(scale):
                    case_parts.append(f'WHEN "{dim}" < {threshold} THEN {j}')
                case_parts.append(f"ELSE {len(scale)}")
                select_parts.append(
                    f"CASE {' '.join(case_parts)} END AS __cell_{i}"
                )

        sql = f"SELECT {', '.join(select_parts)} FROM \"{table_name}\""
        rows = con.execute(sql).fetchall()

        for row in rows:
            rid = row[0]
            cell = tuple(row[1:])
            if cell not in self._row_id_buckets:
                self._row_id_buckets[cell] = []
            self._row_id_buckets[cell].append(rid)

        # Convert lists to numpy arrays
        for cell in self._row_id_buckets:
            self._row_id_buckets[cell] = np.array(
                self._row_id_buckets[cell], dtype=np.int64
            )

    def _cell_range(self, dim_idx: int, low: float, high: float) -> list[int]:
        """Return which cell indices along a dimension overlap [low, high]."""
        scale = self.scales[dim_idx]
        n_cells = len(scale) + 1

        # Find first cell that could contain values >= low
        start = 0
        for i, threshold in enumerate(scale):
            if low < threshold:
                start = i
                break
            start = i + 1

        # Find last cell that could contain values <= high
        end = n_cells - 1
        for i, threshold in enumerate(scale):
            if high < threshold:
                end = i
                break

        return list(range(start, end + 1))

    def range_query(self, bounds: dict[str, tuple[float, float]]) -> list[int]:
        """Query by ranges. bounds = {"col": (low, high), ...}."""
        if not self._row_id_buckets:
            return []

        # Get cell ranges per dimension
        cell_ranges: list[list[int]] = []
        for i, dim in enumerate(self.dimensions):
            if dim in bounds:
                low, high = bounds[dim]
                cell_ranges.append(self._cell_range(i, low, high))
            else:
                # No filter on this dim — all cells
                cell_ranges.append(list(range(len(self.scales[i]) + 1)))

        # Cartesian product of cell ranges
        from itertools import product

        result: list[int] = []
        for cell in product(*cell_ranges):
            if cell in self._row_id_buckets:
                result.extend(self._row_id_buckets[cell].tolist())

        return result
