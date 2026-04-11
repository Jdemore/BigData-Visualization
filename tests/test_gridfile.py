"""Tests for Grid file index."""

import tempfile

from lava.index.builder import build_gridfile_index
from lava.index.gridfile import GridFile


class TestGridFile:
    def test_build(self, sales_table):
        with tempfile.TemporaryDirectory() as tmp:
            grid = GridFile(bucket_dir=tmp)
            grid.build(sales_table, "test_sales", ["quantity", "revenue"])
            assert len(grid.dimensions) == 2
            assert len(grid.scales) == 2

    def test_range_query(self, sales_table):
        with tempfile.TemporaryDirectory() as tmp:
            grid = GridFile(bucket_dir=tmp)
            grid.build(
                sales_table, "test_sales", ["quantity", "revenue"],
                target_bucket_size=20,
            )
            # Query all — should get all 100 rows
            all_results = grid.range_query({
                "quantity": (0.0, 1e9),
                "revenue": (0.0, 1e9),
            })
            assert len(all_results) == 100

    def test_single_dim(self, sales_table):
        with tempfile.TemporaryDirectory() as tmp:
            grid = GridFile(bucket_dir=tmp)
            grid.build(sales_table, "test_sales", ["revenue"])
            results = grid.range_query({"revenue": (0.0, 1e9)})
            assert len(results) == 100


class TestGridFileBuilder:
    def test_builder(self, sales_table):
        with tempfile.TemporaryDirectory() as tmp:
            grid = build_gridfile_index(
                sales_table, "test_sales", ["quantity", "revenue"], tmp
            )
            results = grid.range_query({
                "quantity": (0.0, 1e9),
                "revenue": (0.0, 1e9),
            })
            assert len(results) == 100
