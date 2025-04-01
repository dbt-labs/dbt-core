from unittest import mock

import pytest

from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationConfig
from dbt.tests.util import run_dbt, write_config_file
from dbt_common.exceptions import DbtValidationError

writable_integration_1 = {
    "name": "writable_integration_1",
    "external_volume": "writable_external_volume",
    "table_format": "writable_format",
    "catalog_type": "writable",
    "adapter_properties": {"my_custom_property": "foo"},
}

writable_integration_2 = {
    "name": "writable_integration_2",
    "external_volume": "writable_external_volume",
    "table_format": "writable_format",
    "catalog_type": "writable",
    "adapter_properties": {"my_custom_property": "bar"},
}

readonly_integration_1 = {
    "name": "readonly_integration_1",
    "external_volume": "readonly_external_volume",
    "table_format": "readonly_format",
    "catalog_type": "readonly",
    "adapter_properties": {"my_custom_property": "baz"},
}


class WritableCatalogIntegration(CatalogIntegration):
    catalog_type = "writable"
    allows_writes = True

    def __init__(self, config: CatalogIntegrationConfig):
        super().__init__(config)
        for key, value in config.adapter_properties.items():
            setattr(self, key, value)


class ReadOnlyCatalogIntegration(CatalogIntegration):
    catalog_type = "readonly"
    allows_writes = False

    def __init__(self, config: CatalogIntegrationConfig):
        super().__init__(config)
        for key, value in config.adapter_properties.items():
            setattr(self, key, value)


class TestSingleWriteIntegration:
    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {"name": "writable_catalog", "write_integrations": [writable_integration_1]},
                {"name": "readonly_catalog", "write_integrations": [readonly_integration_1]},
            ]
        }

    def test_integration(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with mock.patch.object(
            type(project.adapter),
            "CATALOG_INTEGRATIONS",
            [WritableCatalogIntegration, ReadOnlyCatalogIntegration],
        ):
            run_dbt(["run"])

            writable_integration = project.adapter.get_catalog_integration("writable_catalog")
            assert isinstance(writable_integration, WritableCatalogIntegration)
            assert writable_integration.name == "writable_catalog"
            assert writable_integration.catalog_type == "writable"
            assert writable_integration.catalog_name == "writable_integration_1"
            assert writable_integration.table_format == "writable_format"
            assert writable_integration.external_volume == "writable_external_volume"
            assert writable_integration.allows_writes is True
            assert writable_integration.my_custom_property == "foo"

            readonly_integration = project.adapter.get_catalog_integration("readonly_catalog")
            assert isinstance(readonly_integration, ReadOnlyCatalogIntegration)
            assert readonly_integration.name == "readonly_catalog"
            assert readonly_integration.catalog_type == "readonly"
            assert readonly_integration.catalog_name == "readonly_integration_1"
            assert readonly_integration.table_format == "readonly_format"
            assert readonly_integration.external_volume == "readonly_external_volume"
            assert readonly_integration.allows_writes is False
            assert readonly_integration.my_custom_property == "baz"


class TestMultipleWriteIntegration:
    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "writable_catalog",
                    "write_integrations": [writable_integration_1, writable_integration_2],
                    "active_write_integration": "writable_integration_2",
                },
            ]
        }

    def test_integration(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with mock.patch.object(
            type(project.adapter), "CATALOG_INTEGRATIONS", [WritableCatalogIntegration]
        ):
            run_dbt(["build"])

            writable_integration = project.adapter.get_catalog_integration("writable_catalog")
            assert isinstance(writable_integration, WritableCatalogIntegration)
            assert writable_integration.name == "writable_catalog"
            assert writable_integration.catalog_type == "writable"
            assert writable_integration.catalog_name == "writable_integration_2"
            assert writable_integration.table_format == "writable_format"
            assert writable_integration.external_volume == "writable_external_volume"
            assert writable_integration.allows_writes is True
            assert writable_integration.my_custom_property == "bar"


class TestNoActiveWriteIntegration:
    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "writable_catalog",
                    "write_integrations": [writable_integration_1, writable_integration_2],
                },
            ]
        }

    def test_integration(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with mock.patch.object(
            type(project.adapter), "CATALOG_INTEGRATIONS", [WritableCatalogIntegration]
        ):
            error_msg = "Catalog 'writable_catalog' must specify an 'active_write_integration' when multiple 'write_integrations' are provided."
            with pytest.raises(DbtValidationError, match=error_msg):
                run_dbt(["run"])


class TestInvalidWriteIntegration:
    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "writable_catalog",
                    "write_integrations": [writable_integration_1, writable_integration_2],
                    "active_write_integration": "writable_integration_3",
                },
            ]
        }

    def test_integration(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with mock.patch.object(
            type(project.adapter), "CATALOG_INTEGRATIONS", [WritableCatalogIntegration]
        ):
            error_msg = "Catalog 'writable_catalog' must specify an 'active_write_integration' from its set of defined 'write_integrations'"
            with pytest.raises(DbtValidationError, match=error_msg):
                run_dbt(["run"])


class TestDuplicateWriteIntegration:
    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "writable_catalog",
                    "write_integrations": [writable_integration_1, writable_integration_1],
                    "active_write_integration": "writable_integration_1",
                },
            ]
        }

    def test_integration(self, project, catalogs, adapter):
        write_config_file(catalogs, project.project_root, "catalogs.yml")

        with mock.patch.object(
            type(project.adapter), "CATALOG_INTEGRATIONS", [WritableCatalogIntegration]
        ):
            error_msg = "Catalog 'writable_catalog' cannot have multiple 'write_integrations' with the same name: 'writable_integration_1'."
            with pytest.raises(DbtValidationError, match=error_msg):
                run_dbt(["run"])
