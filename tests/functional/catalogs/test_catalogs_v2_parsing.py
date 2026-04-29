"""Functional tests for catalogs.yml v2 parsing and integration."""

from dataclasses import dataclass
from typing import Optional
from unittest import mock

import pytest

from dbt.adapters.catalogs import (
    CatalogIntegration,
    CatalogIntegrationConfig,
    register_catalog_config,
)
from dbt.adapters.catalogs._v2_registry import _REGISTRY
from dbt.tests.util import run_dbt, write_config_file
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.exceptions import DbtValidationError

# ===== Stub integrations matching the v2-to-v1 bridge catalog_type strings =====


class BuiltInStubIntegration(CatalogIntegration):
    catalog_type = "BUILT_IN"

    def __init__(self, config: CatalogIntegrationConfig):
        super().__init__(config)
        for key, value in config.adapter_properties.items():
            setattr(self, key, value)


class IcebergRestStubIntegration(CatalogIntegration):
    catalog_type = "ICEBERG_REST"

    def __init__(self, config: CatalogIntegrationConfig):
        super().__init__(config)
        for key, value in config.adapter_properties.items():
            setattr(self, key, value)


V2_STUB_INTEGRATIONS = [BuiltInStubIntegration, IcebergRestStubIntegration]


# ===== Stub schemas registered in the v2 catalog config registry =====
# Real adapter packages register concrete schemas (HorizonSnowflakeConfig etc.); these
# functional tests use permissive stubs to exercise the parse → validate → bridge flow
# without depending on any specific adapter's installed version.


@dataclass
class _StubPlatformConfig(dbtClassMixin):
    external_volume: Optional[str] = None
    change_tracking: Optional[bool] = None
    base_location_root: Optional[str] = None
    catalog_database: Optional[str] = None
    auto_refresh: Optional[bool] = None
    target_file_size: Optional[str] = None


@pytest.fixture(autouse=True)
def _register_stub_v2_configs():
    snapshot = dict(_REGISTRY)
    for ct in ("horizon", "glue", "iceberg_rest", "unity"):
        register_catalog_config(ct, "snowflake", _StubPlatformConfig)
    yield
    _REGISTRY.clear()
    _REGISTRY.update(snapshot)


def _mock_adapter_type(adapter_type):
    """Mock the bridge and validation functions to use a specific adapter_type.

    This avoids mocking credentials.type which would break internal adapter validation.
    """
    from dbt.config.catalogs import bridge_v2_catalog_to_integration as _real_bridge
    from dbt.config.catalogs import validate_v2_catalog_for_platform as _real_validate

    return (
        mock.patch(
            "dbt.cli.requires.validate_v2_catalog_for_platform",
            side_effect=lambda cat, _: _real_validate(cat, adapter_type),
        ),
        mock.patch(
            "dbt.cli.requires.bridge_v2_catalog_to_integration",
            side_effect=lambda cat, _: _real_bridge(cat, adapter_type),
        ),
    )


# ===== Happy-path integration tests =====


class TestV2HorizonIntegration:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "sf_managed",
                    "type": "horizon",
                    "table_format": "iceberg",
                    "config": {
                        "snowflake": {
                            "external_volume": "s3_iceberg",
                            "change_tracking": True,
                            "base_location_root": "analytics/iceberg",
                        }
                    },
                }
            ]
        }

    def test_horizon_registers_as_built_in(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")
        mock_validate, mock_bridge = _mock_adapter_type("snowflake")

        with (
            mock_validate,
            mock_bridge,
            mock.patch.object(type(project.adapter), "CATALOG_INTEGRATIONS", V2_STUB_INTEGRATIONS),
        ):
            run_dbt(["run"])

            integration = project.adapter.get_catalog_integration("sf_managed")
            assert isinstance(integration, BuiltInStubIntegration)
            assert integration.name == "sf_managed"
            assert integration.catalog_name == "sf_managed"
            assert integration.catalog_type == "BUILT_IN"
            assert integration.table_format == "ICEBERG"
            assert integration.external_volume == "s3_iceberg"
            assert integration.change_tracking is True
            assert integration.base_location_root == "analytics/iceberg"


class TestV2GlueIntegration:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "glue_cat",
                    "type": "glue",
                    "table_format": "iceberg",
                    "config": {
                        "snowflake": {
                            "catalog_database": "MY_GLUE_CLD",
                            "auto_refresh": True,
                            "target_file_size": "AUTO",
                        }
                    },
                }
            ]
        }

    def test_glue_registers_as_iceberg_rest(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")
        mock_validate, mock_bridge = _mock_adapter_type("snowflake")

        with (
            mock_validate,
            mock_bridge,
            mock.patch.object(type(project.adapter), "CATALOG_INTEGRATIONS", V2_STUB_INTEGRATIONS),
        ):
            run_dbt(["run"])

            integration = project.adapter.get_catalog_integration("glue_cat")
            assert isinstance(integration, IcebergRestStubIntegration)
            assert integration.catalog_type == "ICEBERG_REST"
            assert integration.catalog_name == "glue_cat"
            # v2 field translation: catalog_database → catalog_linked_database
            assert integration.catalog_linked_database == "MY_GLUE_CLD"
            assert integration.catalog_linked_database_type == "glue"
            assert integration.auto_refresh is True
            assert integration.target_file_size == "AUTO"


class TestV2MultipleCatalogs:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "horizon_cat",
                    "type": "horizon",
                    "table_format": "iceberg",
                    "config": {"snowflake": {"external_volume": "vol1"}},
                },
                {
                    "name": "glue_cat",
                    "type": "glue",
                    "table_format": "iceberg",
                    "config": {"snowflake": {"catalog_database": "DB1"}},
                },
            ]
        }

    def test_multiple_catalogs_registered(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")
        mock_validate, mock_bridge = _mock_adapter_type("snowflake")

        with (
            mock_validate,
            mock_bridge,
            mock.patch.object(type(project.adapter), "CATALOG_INTEGRATIONS", V2_STUB_INTEGRATIONS),
        ):
            run_dbt(["run"])

            horizon = project.adapter.get_catalog_integration("horizon_cat")
            assert isinstance(horizon, BuiltInStubIntegration)
            assert horizon.external_volume == "vol1"

            glue = project.adapter.get_catalog_integration("glue_cat")
            assert isinstance(glue, IcebergRestStubIntegration)
            assert glue.catalog_linked_database == "DB1"


# ===== Validation error tests =====


class TestV2DuplicateCatalogNames:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "dup_cat",
                    "type": "horizon",
                    "table_format": "iceberg",
                    "config": {"snowflake": {"external_volume": "vol"}},
                },
                {
                    "name": "dup_cat",
                    "type": "glue",
                    "table_format": "iceberg",
                    "config": {"snowflake": {"catalog_database": "DB"}},
                },
            ]
        }

    def test_duplicate_names_rejected(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with pytest.raises(DbtValidationError, match="Duplicate catalog name"):
            run_dbt(["run"])


class TestV2InvalidType:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "bad_cat",
                    "type": "nonexistent_type",
                    "table_format": "iceberg",
                    "config": {},
                }
            ]
        }

    def test_invalid_type_rejected(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with pytest.raises(DbtValidationError, match="Invalid catalog type"):
            run_dbt(["run"])


class TestV2InvalidTableFormat:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "bad_cat",
                    "type": "horizon",
                    "table_format": "parquet",
                    "config": {},
                }
            ]
        }

    def test_invalid_table_format_rejected(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with pytest.raises(DbtValidationError, match="Invalid table_format"):
            run_dbt(["run"])


class TestV2UnknownTopLevelKey:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "cat",
                    "type": "horizon",
                    "table_format": "iceberg",
                    "config": {},
                    "write_integrations": [],
                }
            ]
        }

    def test_unknown_key_rejected(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with pytest.raises(DbtValidationError, match="Unknown keys"):
            run_dbt(["run"])


class TestV2MissingRequiredKey:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "cat",
                    "type": "horizon",
                    "table_format": "iceberg",
                    # missing "config"
                }
            ]
        }

    def test_missing_config_rejected(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with pytest.raises(DbtValidationError, match="Missing required key 'config'"):
            run_dbt(["run"])


class TestV2UnknownPlatformKey:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "cat",
                    "type": "horizon",
                    "table_format": "iceberg",
                    "config": {"oracle": {"some_field": "val"}},
                }
            ]
        }

    def test_unknown_platform_rejected(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with pytest.raises(DbtValidationError, match="unknown platform keys"):
            run_dbt(["run"])


class TestV2EmptyCatalogsWorks:
    """With use_catalogs_v2 enabled but no catalogs.yml, dbt should still run fine."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    def test_no_catalogs_file(self, project, adapter):
        run_dbt(["run"])
