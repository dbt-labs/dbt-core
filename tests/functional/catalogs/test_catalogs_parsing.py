from unittest import mock

import pytest

from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationConfig
from dbt.tests.util import run_dbt, write_config_file
from dbt_common.exceptions import DbtValidationError

write_integration_1 = {
    "name": "write_integration_1",
    "external_volume": "write_external_volume",
    "table_format": "write_format",
    "catalog_type": "write",
    "adapter_properties": {"my_custom_property": "foo_1"},
}

write_integration_2 = {
    "name": "write_integration_2",
    "external_volume": "write_external_volume",
    "table_format": "write_format",
    "catalog_type": "write",
    "adapter_properties": {"my_custom_property": "foo_2"},
}


class WriteCatalogIntegration(CatalogIntegration):
    catalog_type = "write"
    allows_writes = True

    def __init__(self, config: CatalogIntegrationConfig):
        super().__init__(config)
        for key, value in config.adapter_properties.items():
            setattr(self, key, value)


class TestSingleWriteIntegration:
    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {"name": "write_catalog_1", "write_integrations": [write_integration_1]},
                {"name": "write_catalog_2", "write_integrations": [write_integration_2]},
            ]
        }

    def test_integration(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with mock.patch.object(
            type(project.adapter), "CATALOG_INTEGRATIONS", [WriteCatalogIntegration]
        ):
            run_dbt(["run"])

            for i in range(1, 3):
                write_integration = project.adapter.get_catalog_integration(f"write_catalog_{i}")
                assert isinstance(write_integration, WriteCatalogIntegration)
                assert write_integration.name == f"write_catalog_{i}"
                assert write_integration.catalog_type == "write"
                assert write_integration.catalog_name == f"write_integration_{i}"
                assert write_integration.table_format == "write_format"
                assert write_integration.external_volume == "write_external_volume"
                assert write_integration.allows_writes is True
                assert write_integration.my_custom_property == f"foo_{i}"


class TestMultipleWriteIntegration:
    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "write_catalog",
                    "write_integrations": [write_integration_1, write_integration_2],
                    "active_write_integration": "write_integration_2",
                },
            ]
        }

    def test_integration(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with mock.patch.object(
            type(project.adapter), "CATALOG_INTEGRATIONS", [WriteCatalogIntegration]
        ):
            run_dbt(["build"])

            write_integration = project.adapter.get_catalog_integration("write_catalog")
            assert write_integration.name == "write_catalog"
            assert write_integration.catalog_name == "write_integration_2"
            assert write_integration.my_custom_property == "foo_2"


class TestNoActiveWriteIntegration:
    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "write_catalog",
                    "write_integrations": [write_integration_1, write_integration_2],
                },
            ]
        }

    def test_integration(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with mock.patch.object(
            type(project.adapter), "CATALOG_INTEGRATIONS", [WriteCatalogIntegration]
        ):
            error_msg = "Catalog 'write_catalog' must specify an 'active_write_integration' when multiple 'write_integrations' are provided."
            with pytest.raises(DbtValidationError, match=error_msg):
                run_dbt(["run"])


class TestInvalidWriteIntegration:
    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "write_catalog",
                    "write_integrations": [write_integration_1, write_integration_2],
                    "active_write_integration": "write_integration_3",
                },
            ]
        }

    def test_integration(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with mock.patch.object(
            type(project.adapter), "CATALOG_INTEGRATIONS", [WriteCatalogIntegration]
        ):
            error_msg = "Catalog 'write_catalog' must specify an 'active_write_integration' from its set of defined 'write_integrations'"
            with pytest.raises(DbtValidationError, match=error_msg):
                run_dbt(["run"])


class TestDuplicateWriteIntegration:
    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "write_catalog",
                    "write_integrations": [write_integration_1, write_integration_1],
                    "active_write_integration": "write_integration_1",
                },
            ]
        }

    def test_integration(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with mock.patch.object(
            type(project.adapter), "CATALOG_INTEGRATIONS", [WriteCatalogIntegration]
        ):
            error_msg = "Catalog 'write_catalog' cannot have multiple 'write_integrations' with the same name: 'write_integration_1'."
            with pytest.raises(DbtValidationError, match=error_msg):
                run_dbt(["run"])
