from unittest import mock

import pytest

from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationConfig
from dbt.tests.util import run_dbt, write_config_file


class TestCatalogIntegration(CatalogIntegration):
    def __init__(self, config: CatalogIntegrationConfig):
        super().__init__(config)

        # adapter properties are not set by default, so set them on integration for testing
        for key, value in config.adapter_properties.items():
            setattr(self, key, value)


class TestCatalogsParsing:
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
            project.adapter.CATALOG_INTEGRATIONS, {"glue": TestCatalogIntegration}
        ):
            run_dbt(["run"])
            foo = project.adapter.get_catalog_integration("my_write_integration")
            assert isinstance(foo, TestCatalogIntegration)
            assert foo.name == "my_write_integration"
            assert foo.external_volume == "my_external_volume"
            assert foo.table_format == "iceberg"
            assert foo.catalog_type == "glue"
            assert foo.my_custom_property == "my_custom_value"
