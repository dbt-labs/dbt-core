"""Functional tests for catalogs.yml v2 parsing and bridge flow."""

from unittest import mock

import pytest

from dbt.adapters.capability import (
    Capability,
    CapabilityDict,
    CapabilitySupport,
    Support,
)
from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationConfig
from dbt.tests.util import run_dbt, write_config_file
from dbt_common.exceptions import DbtValidationError


class StubCatalogIntegration(CatalogIntegration):
    """Minimal integration that accepts any catalog_type and stores adapter_properties."""

    catalog_type = "stub"
    allows_writes = True

    def __init__(self, config: CatalogIntegrationConfig):
        super().__init__(config)
        for key, value in config.adapter_properties.items():
            setattr(self, key, value)


def _v2_capabilities():
    return CapabilityDict({Capability.CatalogsV2: CapabilitySupport(support=Support.Full)})  # type: ignore[attr-defined]


def _mock_bridge(catalog_type: str):
    """Return a bridge side_effect that produces a CatalogWriteIntegrationConfig
    with catalog_type='stub' so StubCatalogIntegration is registered."""
    from dbt.artifacts.resources import CatalogWriteIntegrationConfig

    def _bridge(self, catalog):
        platform_block = catalog.config.get("snowflake", {}) or {}
        external_volume = platform_block.get("external_volume")
        file_format = platform_block.get("file_format")
        props = {
            k: v for k, v in platform_block.items() if k not in {"external_volume", "file_format"}
        }
        return CatalogWriteIntegrationConfig(
            name=catalog.name,
            catalog_type="stub",
            catalog_name=catalog.name,
            table_format=catalog.table_format.value,
            external_volume=str(external_volume) if external_volume is not None else None,
            file_format=str(file_format) if file_format is not None else None,
            adapter_properties=props,
        )

    return _bridge


class TestV2HappyPath:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "my_catalog",
                    "type": "horizon",
                    "table_format": "iceberg",
                    "config": {
                        "snowflake": {
                            "external_volume": "s3_vol",
                            "change_tracking": True,
                        }
                    },
                }
            ]
        }

    def test_catalog_registered(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with (
            mock.patch.object(
                type(project.adapter), "CATALOG_INTEGRATIONS", [StubCatalogIntegration]
            ),
            mock.patch.object(type(project.adapter), "_capabilities", _v2_capabilities()),
            mock.patch.object(type(project.adapter), "bridge_v2_catalog", _mock_bridge("horizon")),
        ):
            run_dbt(["run"])
            integration = project.adapter.get_catalog_integration("my_catalog")
            assert isinstance(integration, StubCatalogIntegration)
            assert integration.name == "my_catalog"
            assert integration.table_format == "iceberg"
            assert integration.external_volume == "s3_vol"
            assert integration.change_tracking is True


class TestV2MultipleCatalogs:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "cat_a",
                    "type": "horizon",
                    "table_format": "iceberg",
                    "config": {"snowflake": {"external_volume": "vol_a"}},
                },
                {
                    "name": "cat_b",
                    "type": "glue",
                    "table_format": "iceberg",
                    "config": {"snowflake": {"external_volume": "vol_b"}},
                },
            ]
        }

    def test_multiple_catalogs_registered(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with (
            mock.patch.object(
                type(project.adapter), "CATALOG_INTEGRATIONS", [StubCatalogIntegration]
            ),
            mock.patch.object(type(project.adapter), "_capabilities", _v2_capabilities()),
            mock.patch.object(type(project.adapter), "bridge_v2_catalog", _mock_bridge("horizon")),
        ):
            run_dbt(["run"])
            assert project.adapter.get_catalog_integration("cat_a").external_volume == "vol_a"
            assert project.adapter.get_catalog_integration("cat_b").external_volume == "vol_b"


class TestV2NoCatalogsFile:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    def test_no_catalogs_file_runs(self, project, adapter):
        with (mock.patch.object(type(project.adapter), "_capabilities", _v2_capabilities()),):
            run_dbt(["run"])


class TestV2DuplicateCatalogNames:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "dup",
                    "type": "horizon",
                    "table_format": "iceberg",
                    "config": {"snowflake": {"external_volume": "vol"}},
                },
                {
                    "name": "dup",
                    "type": "glue",
                    "table_format": "iceberg",
                    "config": {"snowflake": {"external_volume": "vol"}},
                },
            ]
        }

    def test_duplicate_names_rejected(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with (mock.patch.object(type(project.adapter), "_capabilities", _v2_capabilities()),):
            with pytest.raises(DbtValidationError, match="Duplicate catalog name"):
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
                    "name": "cat",
                    "type": "horizon",
                    "table_format": "parquet",
                    "config": {},
                }
            ]
        }

    def test_invalid_table_format_rejected(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with (mock.patch.object(type(project.adapter), "_capabilities", _v2_capabilities()),):
            with pytest.raises(DbtValidationError, match="Invalid table_format"):
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
                    # missing config
                }
            ]
        }

    def test_missing_config_rejected(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with (mock.patch.object(type(project.adapter), "_capabilities", _v2_capabilities()),):
            with pytest.raises(DbtValidationError, match="Missing required key 'config'"):
                run_dbt(["run"])
