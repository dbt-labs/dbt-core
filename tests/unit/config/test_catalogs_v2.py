import pytest

from dbt.artifacts.resources import V2TableFormat
from dbt.config.catalogs import load_catalogs_v2, load_single_catalog_v2
from dbt.config.renderer import SecretRenderer
from dbt_common.exceptions import DbtValidationError


@pytest.fixture
def renderer():
    return SecretRenderer({})


# ===== load_catalogs_v2 file-level validation =====


class TestLoadCatalogsV2FileLevel:
    def test_rejects_iceberg_catalogs_key(self, tmp_path):
        (tmp_path / "catalogs.yml").write_text("iceberg_catalogs:\n  - name: cat\n")
        with pytest.raises(DbtValidationError, match="uses 'catalogs', not 'iceberg_catalogs'"):
            load_catalogs_v2(str(tmp_path), "test_project", {})

    def test_rejects_unknown_top_level_keys(self, tmp_path):
        (tmp_path / "catalogs.yml").write_text(
            "version: 2\ncatalogs:\n  - name: cat\n    type: horizon\n"
            "    table_format: iceberg\n    config: {}\n"
        )
        with pytest.raises(DbtValidationError, match="Unknown top-level keys.*version"):
            load_catalogs_v2(str(tmp_path), "test_project", {})

    def test_empty_file_returns_empty(self, tmp_path):
        result = load_catalogs_v2(str(tmp_path), "test_project", {})
        assert result == []

    def test_duplicate_names_rejected(self, tmp_path):
        (tmp_path / "catalogs.yml").write_text(
            "catalogs:\n"
            "  - name: dup\n    type: horizon\n    table_format: iceberg\n    config: {}\n"
            "  - name: dup\n    type: glue\n    table_format: iceberg\n    config: {}\n"
        )
        with pytest.raises(DbtValidationError, match="Duplicate catalog name"):
            load_catalogs_v2(str(tmp_path), "test_project", {})


# ===== load_single_catalog_v2 structural validation =====


class TestLoadSingleCatalogV2:
    def test_valid_minimal(self, renderer):
        raw = {"name": "cat", "type": "horizon", "table_format": "iceberg", "config": {}}
        catalog = load_single_catalog_v2(raw, renderer)
        assert catalog.name == "cat"
        assert catalog.catalog_type == "horizon"
        assert catalog.table_format == V2TableFormat.ICEBERG
        assert catalog.config == {}

    def test_valid_with_platform_block(self, renderer):
        raw = {
            "name": "sf_cat",
            "type": "horizon",
            "table_format": "iceberg",
            "config": {"snowflake": {"external_volume": "vol", "change_tracking": True}},
        }
        catalog = load_single_catalog_v2(raw, renderer)
        assert catalog.config == {"snowflake": {"external_volume": "vol", "change_tracking": True}}

    def test_valid_multi_platform(self, renderer):
        raw = {
            "name": "unity_cat",
            "type": "unity",
            "table_format": "iceberg",
            "config": {
                "snowflake": {"catalog_database": "MY_DB"},
                "databricks": {"file_format": "delta"},
            },
        }
        catalog = load_single_catalog_v2(raw, renderer)
        assert catalog.config["snowflake"] == {"catalog_database": "MY_DB"}
        assert catalog.config["databricks"] == {"file_format": "delta"}

    def test_type_normalized_to_lowercase(self, renderer):
        raw = {"name": "cat", "type": "HORIZON", "table_format": "ICEBERG", "config": {}}
        catalog = load_single_catalog_v2(raw, renderer)
        assert catalog.catalog_type == "horizon"
        assert catalog.table_format == V2TableFormat.ICEBERG

    def test_unknown_type_accepted(self, renderer):
        raw = {"name": "cat", "type": "my_custom_type", "table_format": "iceberg", "config": {}}
        catalog = load_single_catalog_v2(raw, renderer)
        assert catalog.catalog_type == "my_custom_type"

    def test_any_platform_key_accepted(self, renderer):
        raw = {
            "name": "cat",
            "type": "my_type",
            "table_format": "iceberg",
            "config": {"oracle": {"some_field": "val"}, "mysql": {"other": 1}},
        }
        catalog = load_single_catalog_v2(raw, renderer)
        assert "oracle" in catalog.config
        assert "mysql" in catalog.config

    def test_missing_name(self, renderer):
        raw = {"type": "horizon", "table_format": "iceberg", "config": {}}
        with pytest.raises(DbtValidationError, match="Missing required key 'name'"):
            load_single_catalog_v2(raw, renderer)

    def test_missing_type(self, renderer):
        raw = {"name": "cat", "table_format": "iceberg", "config": {}}
        with pytest.raises(DbtValidationError, match="Missing required key 'type'"):
            load_single_catalog_v2(raw, renderer)

    def test_missing_table_format(self, renderer):
        raw = {"name": "cat", "type": "horizon", "config": {}}
        with pytest.raises(DbtValidationError, match="Missing required key 'table_format'"):
            load_single_catalog_v2(raw, renderer)

    def test_missing_config(self, renderer):
        raw = {"name": "cat", "type": "horizon", "table_format": "iceberg"}
        with pytest.raises(DbtValidationError, match="Missing required key 'config'"):
            load_single_catalog_v2(raw, renderer)

    def test_empty_name_rejected(self, renderer):
        raw = {"name": "  ", "type": "horizon", "table_format": "iceberg", "config": {}}
        with pytest.raises(DbtValidationError, match="non-empty string"):
            load_single_catalog_v2(raw, renderer)

    def test_invalid_table_format(self, renderer):
        raw = {"name": "cat", "type": "horizon", "table_format": "parquet", "config": {}}
        with pytest.raises(DbtValidationError, match="Invalid table_format"):
            load_single_catalog_v2(raw, renderer)

    def test_unknown_top_level_key_rejected(self, renderer):
        raw = {
            "name": "cat",
            "type": "horizon",
            "table_format": "iceberg",
            "config": {},
            "extra": "val",
        }
        with pytest.raises(DbtValidationError, match="Unknown keys.*extra"):
            load_single_catalog_v2(raw, renderer)

    def test_config_not_dict_rejected(self, renderer):
        raw = {"name": "cat", "type": "horizon", "table_format": "iceberg", "config": "bad"}
        with pytest.raises(DbtValidationError, match="config must be a mapping"):
            load_single_catalog_v2(raw, renderer)

    def test_platform_block_not_dict_rejected(self, renderer):
        raw = {
            "name": "cat",
            "type": "horizon",
            "table_format": "iceberg",
            "config": {"snowflake": "bad"},
        }
        with pytest.raises(DbtValidationError, match="config.snowflake must be a mapping"):
            load_single_catalog_v2(raw, renderer)

    def test_table_format_default_accepted(self, renderer):
        raw = {"name": "cat", "type": "hive_metastore", "table_format": "default", "config": {}}
        catalog = load_single_catalog_v2(raw, renderer)
        assert catalog.table_format == V2TableFormat.DEFAULT

    def test_name_stripped(self, renderer):
        raw = {"name": "  my_cat  ", "type": "horizon", "table_format": "iceberg", "config": {}}
        catalog = load_single_catalog_v2(raw, renderer)
        assert catalog.name == "my_cat"
