"""Tests for data ingestion."""

import os
import tempfile

from lava.engine.ingestion import (
    ensure_parquet,
    get_sample_values,
    profile_dataset,
    register_dataset,
)

SALES_CSV = os.path.join(
    os.path.dirname(__file__), "..", "sample_data", "sales.csv"
)


class TestProfileDataset:
    def test_profile_csv(self, con):
        profile = profile_dataset(con, SALES_CSV)
        assert "order_id" in profile["columns"]
        assert "revenue" in profile["columns"]
        assert len(profile["columns"]) == 9
        assert profile["estimated_rows"] == 50_000
        assert profile["size_bytes"] > 0

    def test_profile_parquet(self, con):
        with tempfile.TemporaryDirectory() as tmp:
            pq_path = os.path.join(tmp, "sales.parquet")
            ensure_parquet(con, SALES_CSV, pq_path)
            profile = profile_dataset(con, pq_path)
            assert "order_id" in profile["columns"]
            assert profile["estimated_rows"] == 50_000


class TestEnsureParquet:
    def test_creates_parquet_file(self, con):
        with tempfile.TemporaryDirectory() as tmp:
            pq_path = os.path.join(tmp, "sales.parquet")
            result = ensure_parquet(con, SALES_CSV, pq_path)
            assert os.path.exists(result)
            assert result == pq_path

    def test_skips_parquet_input(self, con):
        result = ensure_parquet(con, "data.parquet", "out.parquet")
        assert result == "data.parquet"


class TestRegisterDataset:
    def test_register_creates_view(self, con):
        with tempfile.TemporaryDirectory() as tmp:
            pq_path = os.path.join(tmp, "sales.parquet")
            ensure_parquet(con, SALES_CSV, pq_path)
            register_dataset(con, "sales", pq_path)
            count = con.execute('SELECT COUNT(*) FROM "sales"').fetchone()[0]
            assert count == 50_000


class TestGetSampleValues:
    def test_returns_samples(self, con):
        samples = get_sample_values(con, SALES_CSV, ["region", "revenue"], n=5)
        assert "region" in samples
        assert "revenue" in samples
        assert len(samples["region"]) <= 5
        assert all(v is not None for v in samples["region"])
