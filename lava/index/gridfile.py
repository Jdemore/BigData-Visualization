"""Grid File: multi-dimensional range index with quantile-based splits.

Unlike a B+-tree, the Grid File handles queries that constrain two or more
numeric columns at once (e.g. lat/lon boxes, or scatter-plot brush selections).
Splits are chosen from column quantiles so every bucket holds roughly
target_bucket_size rows regardless of the underlying distribution.
"""

import os
from dataclasses import dataclass, field

import duckdb
import numpy as np


@dataclass
class GridFile:
    """N-dimensional grid index. One bucket per (cell_0, cell_1, ...) tuple."""

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
        """Compute split points and populate cell->row-ids buckets.

        Quantile splits (DuckDB's quantile_cont) are what make this balanced:
        a naive equal-width grid would be empty in sparse regions and oversized
        in dense ones, defeating the point of the index.
        """
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
            # Dedupe: columns with low cardinality can yield identical quantile values.
            q_values = sorted(set(q_values))
            self.scales.append(q_values)

        os.makedirs(self.bucket_dir, exist_ok=True)

        self._row_id_buckets = {}

        # Assign every row to a cell via a single CASE-based SQL pass.
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

        # Swap Python lists for numpy arrays so range_query has less to copy.
        for cell in self._row_id_buckets:
            self._row_id_buckets[cell] = np.array(
                self._row_id_buckets[cell], dtype=np.int64
            )

    def _cell_range(self, dim_idx: int, low: float, high: float) -> list[int]:
        """Cell indices along one dimension whose value range overlaps [low, high]."""
        scale = self.scales[dim_idx]
        n_cells = len(scale) + 1

        start = 0
        for i, threshold in enumerate(scale):
            if low < threshold:
                start = i
                break
            start = i + 1

        end = n_cells - 1
        for i, threshold in enumerate(scale):
            if high < threshold:
                end = i
                break

        return list(range(start, end + 1))

    def range_query(self, bounds: dict[str, tuple[float, float]]) -> list[int]:
        """Return every row id whose dimension values all fall inside their bound.

        bounds is a partial map -- unspecified dimensions match every cell along
        that axis. The result is the union of row-ids across all selected buckets.
        """
        if not self._row_id_buckets:
            return []

        cell_ranges: list[list[int]] = []
        for i, dim in enumerate(self.dimensions):
            if dim in bounds:
                low, high = bounds[dim]
                cell_ranges.append(self._cell_range(i, low, high))
            else:
                cell_ranges.append(list(range(len(self.scales[i]) + 1)))

        from itertools import product

        result: list[int] = []
        for cell in product(*cell_ranges):
            if cell in self._row_id_buckets:
                result.extend(self._row_id_buckets[cell].tolist())

        return result
