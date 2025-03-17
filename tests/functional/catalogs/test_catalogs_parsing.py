from dataclasses import dataclass
from typing import Any, Dict, Optional
from unittest import mock

import pytest

from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationConfig
from dbt.tests.util import run_dbt, write_config_file


@dataclass
class TestCatalogIntegrationConfig(CatalogIntegrationConfig):
    name: str
    catalog_type: str
    external_volume: Optional[str]
    table_format: str
    adapter_properties: Optional[Dict[str, Any]]


class TestCatalogsParsing:

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "test_catalog",
                    "write_integrations": [
                        {
                            "name": "write_integration_name",
                            "external_volume": "write_integration_external_volume",
                            "table_format": "iceberg",
                            "catalog_type": "glue",
                        }
                    ],
                }
            ]
        }

    def test_catalog_parsing_adapter_initialialization(self, catalogs, project):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        mock_add_catalog_integration = mock.Mock()
        with mock.patch.object(
            type(project.adapter), "add_catalog_integrations", mock_add_catalog_integration
        ):
            run_dbt(["run"])

            mock_add_catalog_integration.assert_called_once_with(
                [
                    CatalogIntegration(
                        TestCatalogIntegrationConfig(
                            name="test_catalog",
                            catalog_type="glue",
                            table_format="iceberg",
                            external_volume="write_integration_external_volume",
                            adapter_properties=None,
                        )
                    )
                ]
            )
