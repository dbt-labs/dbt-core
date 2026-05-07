from dataclasses import dataclass
from typing import Optional
from unittest import mock

import pytest

from dbt.artifacts.resources import CatalogV2, CatalogV2PlatformConfig
from dbt.config.catalogs import (
    bridge_v2_catalog_to_integration,
    load_catalogs_v2,
    load_single_catalog_v2,
    validate_v2_catalog_for_platform,
)
from dbt.config.renderer import SecretRenderer
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.exceptions import DbtValidationError


@dataclass
class _FakePlatformConfig(dbtClassMixin):
    """Permissive fake schema used in framework tests; accepts the field shapes that
    real adapter schemas accept. Schema-content checks live in adapter test suites."""

    # snowflake fields
    external_volume: Optional[str] = None
    change_tracking: Optional[bool] = None
    data_retention_time_in_days: Optional[int] = None
    max_data_extension_time_in_days: Optional[int] = None
    storage_serialization_policy: Optional[str] = None
    base_location_root: Optional[str] = None
    catalog_database: Optional[str] = None
    auto_refresh: Optional[bool] = None
    target_file_size: Optional[str] = None
    # databricks / bigquery fields
    file_format: Optional[str] = None
    location_root: Optional[str] = None
    use_uniform: Optional[bool] = None


class _FakeAdapter:
    """Stub adapter class exposing CATALOG_V2_CONFIGS for framework tests."""

    CATALOG_V2_CONFIGS = {
        "horizon": _FakePlatformConfig,
        "glue": _FakePlatformConfig,
        "iceberg_rest": _FakePlatformConfig,
        "unity": _FakePlatformConfig,
        "hive_metastore": _FakePlatformConfig,
        "biglake_metastore": _FakePlatformConfig,
    }


@pytest.fixture(autouse=True)
def _stub_adapter_lookup():
    """Stub FACTORY.get_adapter_class_by_name so tests don't need real adapters loaded.

    Real adapter packages declare CATALOG_V2_CONFIGS as a class attribute on their
    adapter class (see SnowflakeAdapter etc.); framework tests here only care that
    the lookup is invoked correctly.
    """
    with mock.patch(
        "dbt.config.catalogs.FACTORY.get_adapter_class_by_name",
        return_value=_FakeAdapter,
    ):
        yield


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

    def test_invalid_type(self, renderer):
        raw = {
            "name": "cat",
            "type": "invalid_type",
            "table_format": "iceberg",
            "config": {},
        }
        with pytest.raises(DbtValidationError, match="Invalid catalog type"):
            load_single_catalog_v2(raw, renderer)

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


# ===== validate_v2_catalog_for_platform =====


def _make_catalog(
    name="test_cat",
    catalog_type="horizon",
    table_format="iceberg",
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


class TestValidateV2CatalogForPlatform:
    # --- registry behavior ---
    def test_no_schema_registered_raises_helpful_error(self):
        # Adapter class with empty CATALOG_V2_CONFIGS: no v2 support for any catalog type
        class _NoConfigsAdapter:
            CATALOG_V2_CONFIGS: dict = {}

        cat = _make_catalog(
            catalog_type="horizon",
            snowflake={"external_volume": "vol"},
        )
        with mock.patch(
            "dbt.config.catalogs.FACTORY.get_adapter_class_by_name",
            return_value=_NoConfigsAdapter,
        ):
            with pytest.raises(DbtValidationError, match="no v2 catalog schema registered"):
                validate_v2_catalog_for_platform(cat, "snowflake")

    # --- horizon ---
    def test_horizon_valid(self):
        cat = _make_catalog(
            catalog_type="horizon",
            snowflake={
                "external_volume": "vol",
                "change_tracking": True,
                "data_retention_time_in_days": 7,
                "max_data_extension_time_in_days": 14,
                "storage_serialization_policy": "COMPATIBLE",
                "base_location_root": "path/to/root",
            },
        )
        validate_v2_catalog_for_platform(cat, "snowflake")  # should not raise

    def test_horizon_missing_snowflake_block(self):
        cat = _make_catalog(catalog_type="horizon")
        with pytest.raises(DbtValidationError, match="requires config.snowflake"):
            validate_v2_catalog_for_platform(cat, "snowflake")

    def test_horizon_wrong_table_format(self):
        cat = _make_catalog(
            catalog_type="horizon",
            table_format="default",
            snowflake={"external_volume": "vol"},
        )
        with pytest.raises(DbtValidationError, match="requires table_format='iceberg'"):
            validate_v2_catalog_for_platform(cat, "snowflake")

    def test_horizon_rejects_databricks_block(self):
        cat = _make_catalog(
            catalog_type="horizon",
            snowflake={"external_volume": "vol"},
            databricks={"file_format": "delta"},
        )
        with pytest.raises(DbtValidationError, match="does not support databricks"):
            validate_v2_catalog_for_platform(cat, "snowflake")

    # --- glue ---
    def test_glue_valid(self):
        cat = _make_catalog(
            catalog_type="glue",
            snowflake={
                "catalog_database": "MY_CLD",
                "auto_refresh": True,
                "target_file_size": "AUTO",
            },
        )
        validate_v2_catalog_for_platform(cat, "snowflake")

    # --- iceberg_rest ---
    def test_iceberg_rest_valid(self):
        cat = _make_catalog(
            catalog_type="iceberg_rest",
            snowflake={"catalog_database": "MY_REST_CLD"},
        )
        validate_v2_catalog_for_platform(cat, "snowflake")

    # --- unity ---
    def test_unity_snowflake_only(self):
        cat = _make_catalog(
            catalog_type="unity",
            snowflake={"catalog_database": "DB"},
        )
        validate_v2_catalog_for_platform(cat, "snowflake")

    def test_unity_databricks_only(self):
        cat = _make_catalog(
            catalog_type="unity",
            databricks={"file_format": "parquet"},
        )
        validate_v2_catalog_for_platform(cat, "databricks")

    def test_unity_databricks_uniform(self):
        cat = _make_catalog(
            catalog_type="unity",
            databricks={"file_format": "delta", "use_uniform": True},
        )
        validate_v2_catalog_for_platform(cat, "databricks")

    def test_unity_both_platforms(self):
        cat = _make_catalog(
            catalog_type="unity",
            snowflake={"catalog_database": "DB"},
            databricks={"file_format": "parquet"},
        )
        validate_v2_catalog_for_platform(cat, "snowflake")
        validate_v2_catalog_for_platform(cat, "databricks")

    def test_unity_no_platform(self):
        cat = _make_catalog(catalog_type="unity")
        with pytest.raises(DbtValidationError, match="requires at least one config block"):
            validate_v2_catalog_for_platform(cat, "snowflake")

    def test_unity_rejects_bigquery(self):
        cat = _make_catalog(
            catalog_type="unity",
            snowflake={"catalog_database": "DB"},
            bigquery={"external_volume": "gs://bucket", "file_format": "parquet"},
        )
        with pytest.raises(DbtValidationError, match="does not support bigquery"):
            validate_v2_catalog_for_platform(cat, "snowflake")

    # --- hive_metastore ---
    def test_hive_metastore_valid(self):
        cat = _make_catalog(
            catalog_type="hive_metastore",
            table_format="default",
            databricks={"file_format": "delta"},
        )
        validate_v2_catalog_for_platform(cat, "databricks")

    def test_hive_metastore_missing_databricks(self):
        cat = _make_catalog(
            catalog_type="hive_metastore",
            table_format="default",
        )
        with pytest.raises(DbtValidationError, match="requires config.databricks"):
            validate_v2_catalog_for_platform(cat, "databricks")

    def test_hive_metastore_wrong_table_format(self):
        cat = _make_catalog(
            catalog_type="hive_metastore",
            table_format="iceberg",
            databricks={"file_format": "delta"},
        )
        with pytest.raises(DbtValidationError, match="requires table_format='default'"):
            validate_v2_catalog_for_platform(cat, "databricks")

    # --- biglake_metastore ---
    def test_biglake_valid(self):
        cat = _make_catalog(
            catalog_type="biglake_metastore",
            bigquery={
                "external_volume": "gs://my-bucket",
                "file_format": "parquet",
                "base_location_root": "root",
            },
        )
        validate_v2_catalog_for_platform(cat, "bigquery")

    def test_biglake_missing_bigquery(self):
        cat = _make_catalog(catalog_type="biglake_metastore")
        with pytest.raises(DbtValidationError, match="requires config.bigquery"):
            validate_v2_catalog_for_platform(cat, "bigquery")


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
            table_format="default",
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

    def test_unsupported_adapter_type(self):
        cat = _make_catalog(
            catalog_type="horizon",
            snowflake={"external_volume": "vol"},
        )
        with pytest.raises(DbtValidationError, match="does not support"):
            bridge_v2_catalog_to_integration(cat, "databricks")

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
