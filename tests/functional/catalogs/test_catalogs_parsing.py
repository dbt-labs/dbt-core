from unittest import mock

import pytest

from dbt.adapters.catalogs import (
    CatalogIntegration,
    CatalogIntegrationConfig,
    DbtCatalogIntegrationNotFoundError,
)
from dbt.tests.util import run_dbt, write_config_file


class TestCatalogIntegration(CatalogIntegration):
    def __init__(self, config: CatalogIntegrationConfig):
        super().__init__(config)

        # adapter properties are not set by default, so set them on integration for testing
        for key, value in config.adapter_properties.items():
            setattr(self, key, value)


class TestSingleWriteIntegration:
    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "test_catalog",
                    "write_integrations": [
                        {
                            "name": "my_write_integration",
                            "external_volume": "my_external_volume",
                            "table_format": "iceberg",
                            "catalog_type": "glue",
                            "adapter_properties": {"my_custom_property": "my_custom_value"},
                        }
                    ],
                }
            ]
        }

    def test_catalog_parsing_adapter_initialialization(self, catalogs, project):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with mock.patch.dict(
            project.adapter.CATALOG_INTEGRATIONS,
            {"glue": TestCatalogIntegration, "iceberg": TestCatalogIntegration},
        ):
            run_dbt(["run"])

            write_integration = project.adapter.get_catalog_integration("my_write_integration")
            assert isinstance(write_integration, TestCatalogIntegration)
            assert write_integration.name == "my_write_integration"
            assert write_integration.external_volume == "my_external_volume"
            assert write_integration.table_format == "iceberg"
            assert write_integration.catalog_type == "glue"
            assert write_integration.my_custom_property == "my_custom_value"


class TestMultipleWriteIntegrations:
    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "test_catalog",
                    "active_write_integration": "my_second_write_integration",
                    "write_integrations": [
                        {
                            "name": "my_write_integration",
                            "external_volume": "my_external_volume",
                            "table_format": "iceberg",
                            "catalog_type": "glue",
                            "adapter_properties": {"my_custom_property": "some_value"},
                        },
                        {
                            "name": "my_second_write_integration",
                            "external_volume": "my_other_external_volume",
                            "table_format": "iceberg",
                            "catalog_type": "glue",
                            "adapter_properties": {"my_custom_property": "some_other_value"},
                        },
                    ],
                }
            ]
        }

    def test_catalog_parsing_adapter_initialialization(self, catalogs, project):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with mock.patch.dict(
            project.adapter.CATALOG_INTEGRATIONS, {"glue": TestCatalogIntegration}
        ):
            run_dbt(["run"])
            with pytest.raises(DbtCatalogIntegrationNotFoundError):
                project.adapter.get_catalog_integration("my_write_integration")

            write_integration = project.adapter.get_catalog_integration(
                "my_second_write_integration"
            )
            assert isinstance(write_integration, TestCatalogIntegration)
            assert write_integration.name == "my_second_write_integration"
            assert write_integration.external_volume == "my_other_external_volume"
            assert write_integration.table_format == "iceberg"
            assert write_integration.catalog_type == "glue"
            assert write_integration.my_custom_property == "some_other_value"
