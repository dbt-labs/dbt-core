import pytest

from dbt.artifacts.resources import CatalogV2, CatalogV2PlatformConfig, V2TableFormat
from dbt.config.catalogs import (
    bridge_v2_catalog_to_integration,
    load_catalogs_v2,
    load_single_catalog_v2,
)
from dbt.config.renderer import SecretRenderer
from dbt_common.exceptions import DbtValidationError


@pytest.fixture
def renderer():
    return SecretRenderer({})


# ===== load_catalogs_v2 file-level validation =====


class TestLoadCatalogsV2FileLevel:
    def test_rejects_iceberg_catalogs_key(self, tmp_path):
        catalogs_file = tmp_path / "catalogs.yml"
        catalogs_file.write_text("iceberg_catalogs:\n  - name: cat\n    type: horizon\n")
        with pytest.raises(DbtValidationError, match="uses 'catalogs', not 'iceberg_catalogs'"):
            load_catalogs_v2(str(tmp_path), "test_project", {})

    def test_rejects_unknown_top_level_keys(self, tmp_path):
        catalogs_file = tmp_path / "catalogs.yml"
        catalogs_file.write_text(
            "version: 2\ncatalogs:\n  - name: cat\n    type: horizon\n"
            "    table_format: iceberg\n    config:\n      snowflake:\n"
            "        external_volume: vol\n"
        )
        with pytest.raises(DbtValidationError, match="Unknown top-level keys.*version"):
            load_catalogs_v2(str(tmp_path), "test_project", {})

    def test_empty_file_returns_empty(self, tmp_path):
        # No catalogs.yml file at all
        result = load_catalogs_v2(str(tmp_path), "test_project", {})
        assert result == []


# ===== load_single_catalog_v2 structural validation =====


class TestLoadSingleCatalogV2:
    def test_valid_horizon(self, renderer):
        raw = {
            "name": "sf_managed",
            "type": "horizon",
            "table_format": "iceberg",
            "config": {
                "snowflake": {
                    "external_volume": "s3_iceberg",
                    "change_tracking": True,
                }
            },
        }
        catalog = load_single_catalog_v2(raw, renderer)
        assert catalog.name == "sf_managed"
        assert catalog.catalog_type == "horizon"
        assert catalog.table_format == "iceberg"
        assert catalog.config.snowflake == {
            "external_volume": "s3_iceberg",
            "change_tracking": True,
        }

    def test_valid_unity_multiplatform(self, renderer):
        raw = {
            "name": "unity_cat",
            "type": "unity",
            "table_format": "iceberg",
            "config": {
                "snowflake": {"catalog_database": "MY_DB"},
                "databricks": {"file_format": "delta", "use_uniform": True},
            },
        }
        catalog = load_single_catalog_v2(raw, renderer)
        assert catalog.catalog_type == "unity"
        assert catalog.config.snowflake == {"catalog_database": "MY_DB"}
        assert catalog.config.databricks == {"file_format": "delta", "use_uniform": True}
        assert catalog.config.bigquery is None

    def test_valid_hive_metastore(self, renderer):
        raw = {
            "name": "hive",
            "type": "hive_metastore",
            "table_format": "default",
            "config": {"databricks": {"file_format": "delta"}},
        }
        catalog = load_single_catalog_v2(raw, renderer)
        assert catalog.catalog_type == "hive_metastore"
        assert catalog.table_format == "default"

    def test_valid_biglake(self, renderer):
        raw = {
            "name": "biglake",
            "type": "biglake_metastore",
            "table_format": "iceberg",
            "config": {
                "bigquery": {
                    "external_volume": "gs://my-bucket",
                    "file_format": "parquet",
                }
            },
        }
        catalog = load_single_catalog_v2(raw, renderer)
        assert catalog.catalog_type == "biglake_metastore"

    def test_type_case_insensitive(self, renderer):
        raw = {
            "name": "cat",
            "type": "HORIZON",
            "table_format": "ICEBERG",
            "config": {"snowflake": {"external_volume": "vol"}},
        }
        catalog = load_single_catalog_v2(raw, renderer)
        assert catalog.catalog_type == "horizon"
        assert catalog.table_format == "iceberg"

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

    def test_unknown_type_passes_through(self, renderer):
        # Unknown catalog types are accepted — 3p adapters define their own types
        raw = {
            "name": "cat",
            "type": "my_custom_type",
            "table_format": "iceberg",
            "config": {},
        }
        catalog = load_single_catalog_v2(raw, renderer)
        assert catalog.catalog_type == "my_custom_type"

    def test_invalid_table_format(self, renderer):
        raw = {
            "name": "cat",
            "type": "horizon",
            "table_format": "parquet",
            "config": {},
        }
        with pytest.raises(DbtValidationError, match="Invalid table_format"):
            load_single_catalog_v2(raw, renderer)

    def test_unknown_top_level_key(self, renderer):
        raw = {
            "name": "cat",
            "type": "horizon",
            "table_format": "iceberg",
            "config": {},
            "extra_key": "val",
        }
        with pytest.raises(DbtValidationError, match="Unknown keys.*extra_key"):
            load_single_catalog_v2(raw, renderer)

    def test_unknown_platform_key(self, renderer):
        raw = {
            "name": "cat",
            "type": "horizon",
            "table_format": "iceberg",
            "config": {"unknown_platform": {}},
        }
        with pytest.raises(DbtValidationError, match="unknown platform keys.*unknown_platform"):
            load_single_catalog_v2(raw, renderer)

    def test_empty_name(self, renderer):
        raw = {"name": "  ", "type": "horizon", "table_format": "iceberg", "config": {}}
        with pytest.raises(DbtValidationError, match="non-empty string"):
            load_single_catalog_v2(raw, renderer)

    def test_config_not_dict(self, renderer):
        raw = {"name": "cat", "type": "horizon", "table_format": "iceberg", "config": "bad"}
        with pytest.raises(DbtValidationError, match="config must be a mapping"):
            load_single_catalog_v2(raw, renderer)

    def test_platform_block_not_dict(self, renderer):
        raw = {
            "name": "cat",
            "type": "horizon",
            "table_format": "iceberg",
            "config": {"snowflake": "bad"},
        }
        with pytest.raises(DbtValidationError, match="config.snowflake must be a mapping"):
            load_single_catalog_v2(raw, renderer)

    def test_horizon_requires_iceberg(self, renderer):
        raw = {"name": "cat", "type": "horizon", "table_format": "default", "config": {}}
        with pytest.raises(
            DbtValidationError, match="type 'horizon' requires table_format='iceberg'"
        ):
            load_single_catalog_v2(raw, renderer)

    def test_glue_requires_iceberg(self, renderer):
        raw = {"name": "cat", "type": "glue", "table_format": "default", "config": {}}
        with pytest.raises(
            DbtValidationError, match="type 'glue' requires table_format='iceberg'"
        ):
            load_single_catalog_v2(raw, renderer)

    def test_iceberg_rest_requires_iceberg(self, renderer):
        raw = {"name": "cat", "type": "iceberg_rest", "table_format": "default", "config": {}}
        with pytest.raises(
            DbtValidationError, match="type 'iceberg_rest' requires table_format='iceberg'"
        ):
            load_single_catalog_v2(raw, renderer)

    def test_unity_requires_iceberg(self, renderer):
        raw = {"name": "cat", "type": "unity", "table_format": "default", "config": {}}
        with pytest.raises(
            DbtValidationError, match="type 'unity' requires table_format='iceberg'"
        ):
            load_single_catalog_v2(raw, renderer)

    def test_hive_metastore_requires_default(self, renderer):
        raw = {"name": "cat", "type": "hive_metastore", "table_format": "iceberg", "config": {}}
        with pytest.raises(
            DbtValidationError, match="type 'hive_metastore' requires table_format='default'"
        ):
            load_single_catalog_v2(raw, renderer)

    def test_biglake_metastore_requires_iceberg(self, renderer):
        raw = {"name": "cat", "type": "biglake_metastore", "table_format": "default", "config": {}}
        with pytest.raises(
            DbtValidationError, match="type 'biglake_metastore' requires table_format='iceberg'"
        ):
            load_single_catalog_v2(raw, renderer)

    def test_unknown_type_skips_table_format_check(self, renderer):
        # Unknown types are not validated — 3p adapters define their own constraints
        raw = {"name": "cat", "type": "my_custom_type", "table_format": "default", "config": {}}
        catalog = load_single_catalog_v2(raw, renderer)
        assert catalog.table_format == V2TableFormat.DEFAULT

    def test_unsupported_platform_rejected(self, renderer):
        # horizon is snowflake-only; databricks block should be rejected
        raw = {
            "name": "cat",
            "type": "horizon",
            "table_format": "iceberg",
            "config": {
                "snowflake": {"external_volume": "vol"},
                "databricks": {"file_format": "delta"},
            },
        }
        with pytest.raises(DbtValidationError, match="does not support databricks.*horizon"):
            load_single_catalog_v2(raw, renderer)

    def test_unsupported_platform_hive_metastore(self, renderer):
        # hive_metastore is databricks-only; snowflake block should be rejected
        raw = {
            "name": "cat",
            "type": "hive_metastore",
            "table_format": "default",
            "config": {
                "databricks": {"file_format": "delta"},
                "snowflake": {"external_volume": "vol"},
            },
        }
        with pytest.raises(DbtValidationError, match="does not support snowflake.*hive_metastore"):
            load_single_catalog_v2(raw, renderer)

    def test_unknown_type_skips_platform_support_check(self, renderer):
        # Unknown types can have any platform block
        raw = {
            "name": "cat",
            "type": "my_custom_type",
            "table_format": "iceberg",
            "config": {"snowflake": {}, "databricks": {}},
        }
        catalog = load_single_catalog_v2(raw, renderer)
        assert catalog.catalog_type == "my_custom_type"


def _make_catalog(
    name="test_cat",
    catalog_type="horizon",
    table_format=V2TableFormat.ICEBERG,
    snowflake=None,
    databricks=None,
    bigquery=None,
):
    return CatalogV2(
        name=name,
        catalog_type=catalog_type,
        table_format=table_format,
        config=CatalogV2PlatformConfig(
            snowflake=snowflake,
            databricks=databricks,
            bigquery=bigquery,
        ),
    )


# ===== bridge_v2_catalog_to_integration =====


class TestBridgeV2CatalogToIntegration:
    def test_horizon_snowflake(self):
        cat = _make_catalog(
            catalog_type="horizon",
            snowflake={
                "external_volume": "vol",
                "change_tracking": True,
                "base_location_root": "path",
            },
        )
        config = bridge_v2_catalog_to_integration(cat, "snowflake")
        assert config.name == "test_cat"
        assert config.catalog_name == "test_cat"
        assert config.catalog_type == "BUILT_IN"
        assert config.table_format == "ICEBERG"
        assert config.external_volume == "vol"
        assert config.file_format is None
        assert config.adapter_properties == {
            "change_tracking": True,
            "base_location_root": "path",
        }

    def test_glue_snowflake(self):
        cat = _make_catalog(
            catalog_type="glue",
            snowflake={"catalog_database": "DB", "auto_refresh": True},
        )
        config = bridge_v2_catalog_to_integration(cat, "snowflake")
        assert config.catalog_type == "ICEBERG_REST"
        # v2 "catalog_database" is translated to "catalog_linked_database" for adapter
        assert config.adapter_properties == {
            "catalog_linked_database": "DB",
            "catalog_linked_database_type": "glue",
            "auto_refresh": True,
        }

    def test_iceberg_rest_snowflake(self):
        cat = _make_catalog(
            catalog_type="iceberg_rest",
            snowflake={"catalog_database": "REST_DB", "target_file_size": "AUTO"},
        )
        config = bridge_v2_catalog_to_integration(cat, "snowflake")
        assert config.catalog_type == "ICEBERG_REST"
        # v2 "catalog_database" → "catalog_linked_database"; no database_type for iceberg_rest
        assert config.adapter_properties == {
            "catalog_linked_database": "REST_DB",
            "target_file_size": "AUTO",
        }

    def test_unity_snowflake(self):
        cat = _make_catalog(
            catalog_type="unity",
            snowflake={"catalog_database": "UNITY_DB"},
            databricks={"file_format": "parquet"},
        )
        config = bridge_v2_catalog_to_integration(cat, "snowflake")
        assert config.catalog_type == "ICEBERG_REST"
        assert config.table_format == "ICEBERG"
        assert config.adapter_properties == {
            "catalog_linked_database": "UNITY_DB",
            "catalog_linked_database_type": "unity",
        }

    def test_unity_databricks(self):
        cat = _make_catalog(
            catalog_type="unity",
            snowflake={"catalog_database": "DB"},
            databricks={"file_format": "delta", "location_root": "/path", "use_uniform": True},
        )
        config = bridge_v2_catalog_to_integration(cat, "databricks")
        assert config.catalog_type == "unity"
        assert config.file_format == "delta"
        assert config.adapter_properties == {
            "location_root": "/path",
            "use_uniform": True,
        }

    def test_hive_metastore_databricks(self):
        cat = _make_catalog(
            catalog_type="hive_metastore",
            table_format=V2TableFormat.DEFAULT,
            databricks={"file_format": "delta"},
        )
        config = bridge_v2_catalog_to_integration(cat, "databricks")
        assert config.catalog_type == "hive_metastore"
        assert config.table_format == "default"
        assert config.file_format == "delta"
        assert config.adapter_properties == {}

    def test_biglake_bigquery(self):
        cat = _make_catalog(
            catalog_type="biglake_metastore",
            bigquery={
                "external_volume": "gs://bucket",
                "file_format": "parquet",
                "base_location_root": "root",
            },
        )
        config = bridge_v2_catalog_to_integration(cat, "bigquery")
        assert config.catalog_type == "biglake_metastore"
        assert config.external_volume == "gs://bucket"
        assert config.file_format == "parquet"
        assert config.adapter_properties == {"base_location_root": "root"}

    def test_unknown_type_passes_through_bridge(self):
        # Unknown catalog types bridge using the raw type string as v1 catalog_type
        cat = _make_catalog(
            catalog_type="my_custom_type",
            snowflake={"external_volume": "vol"},
        )
        config = bridge_v2_catalog_to_integration(cat, "snowflake")
        assert config.catalog_type == "my_custom_type"

    def test_missing_platform_block_uses_empty(self):
        """When a unity catalog has no snowflake block, bridge uses empty + type annotation."""
        cat = _make_catalog(
            catalog_type="unity",
            databricks={"file_format": "delta"},
        )
        config = bridge_v2_catalog_to_integration(cat, "snowflake")
        assert config.catalog_type == "ICEBERG_REST"
        # Even with no snowflake block, the linked database type is set
        assert config.adapter_properties == {
            "catalog_linked_database_type": "unity",
        }
