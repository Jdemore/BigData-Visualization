"""Tests for metadata catalog."""

from lava.engine.catalog import catalog_list, catalog_register, init_catalog


class TestCatalog:
    def test_init_creates_table(self, con):
        init_catalog(con)
        result = con.execute(
            "SELECT COUNT(*) FROM __lava_catalog"
        ).fetchone()[0]
        assert result == 0

    def test_register_and_list(self, con):
        init_catalog(con)
        profile = {
            "columns": ["a", "b"],
            "dtypes": {"a": "INTEGER", "b": "VARCHAR"},
            "estimated_rows": 1000,
            "size_bytes": 5000,
        }
        catalog_register(con, "test_ds", "/path/to/test.parquet", profile)
        datasets = catalog_list(con)
        assert len(datasets) == 1
        assert datasets[0]["name"] == "test_ds"
        assert datasets[0]["row_count"] == 1000
