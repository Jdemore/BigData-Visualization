"""Tests for B+-tree index."""

import os
import tempfile

import numpy as np

from lava.index.bptree import PAGE_SIZE, BPlusTree
from lava.index.builder import build_bptree_index


class TestBPlusTree:
    def test_build_and_point_query(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "test.bpt")
            keys = np.arange(10000, dtype=np.float64)
            row_ids = np.arange(10000, dtype=np.int64)
            tree = BPlusTree(path=path)
            tree.build_from_sorted(keys, row_ids)
            result = tree.point_query(500.0)
            assert 500 in result
            tree.close()

    def test_range_query(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "test.bpt")
            keys = np.arange(1000, dtype=np.float64)
            row_ids = np.arange(1000, dtype=np.int64)
            tree = BPlusTree(path=path)
            tree.build_from_sorted(keys, row_ids)
            result = tree.range_query(100.0, 200.0)
            expected = list(range(100, 201))
            assert sorted(result) == expected
            tree.close()

    def test_range_query_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "test.bpt")
            keys = np.arange(100, dtype=np.float64)
            row_ids = np.arange(100, dtype=np.int64)
            tree = BPlusTree(path=path)
            tree.build_from_sorted(keys, row_ids)
            result = tree.range_query(500.0, 600.0)
            assert result == []
            tree.close()

    def test_page_alignment(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "test.bpt")
            keys = np.arange(10000, dtype=np.float64)
            row_ids = np.arange(10000, dtype=np.int64)
            tree = BPlusTree(path=path)
            tree.build_from_sorted(keys, row_ids)
            tree.close()
            file_size = os.path.getsize(path)
            assert file_size % PAGE_SIZE == 0

    def test_large_build_correctness(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "test.bpt")
            n = 500_000
            keys = np.arange(n, dtype=np.float64)
            row_ids = np.arange(n, dtype=np.int64)
            tree = BPlusTree(path=path)
            tree.build_from_sorted(keys, row_ids)
            # Verify a range query against brute-force
            result = tree.range_query(100000.0, 100100.0)
            expected = list(range(100000, 100101))
            assert sorted(result) == expected
            tree.close()


class TestBPlusTreeBuilder:
    def test_builder_on_sales(self, sales_table):
        with tempfile.TemporaryDirectory() as tmp:
            tree = build_bptree_index(
                sales_table, "test_sales", "revenue", tmp
            )
            # Should be able to query
            results = tree.range_query(0.0, 1e9)
            assert len(results) == 100
            tree.close()
