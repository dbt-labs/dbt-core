from unittest import mock

import pytest

from dbt.adapters.catalogs import (
    CatalogIntegration,
    CatalogIntegrationConfig,
    DbtCatalogIntegrationNotFoundError,
)
from dbt.tests.util import run_dbt, write_config_file
from dbt_common.exceptions import DbtValidationError


class TestCatalogIntegration(CatalogIntegration):
    def __init__(self, config: CatalogIntegrationConfig):
        super().__init__(config)
        for key, value in config.adapter_properties.items():
            setattr(self, key, value)


def mock_integrations():
    """Return mock dict for catalog integrations"""
    return {"glue": TestCatalogIntegration, "iceberg": TestCatalogIntegration}


# Common write integration configs for reuse
FIRST_INTEGRATION = {
    "name": "my_write_integration",
    "external_volume": "my_external_volume",
    "table_format": "iceberg",
    "catalog_type": "glue",
    "adapter_properties": {"my_custom_property": "some_value"},
}

SECOND_INTEGRATION = {
    "name": "my_second_write_integration",
    "external_volume": "my_other_external_volume",
    "table_format": "iceberg",
    "catalog_type": "glue",
    "adapter_properties": {"my_custom_property": "some_other_value"},
}


class BaseCatalogTest:
    @pytest.fixture
    def integrations(self):
        """Override in subclasses to provide write_integrations"""
        return []

    @pytest.fixture
    def active_integration(self):
        """Override in subclasses to provide active_write_integration"""
        return None

    @pytest.fixture
    def catalogs(self, integrations, active_integration):
        catalog = {"name": "test_catalog", "write_integrations": integrations}
        if active_integration:
            catalog["active_write_integration"] = active_integration
        return {"catalogs": [catalog]}

    def setup_test(self, catalogs, project):
        write_config_file(catalogs, project.project_root, "catalogs.yml")
        return mock.patch.dict(project.adapter.CATALOG_INTEGRATIONS, mock_integrations())


class TestSingleWriteIntegration(BaseCatalogTest):
    @pytest.fixture
    def integrations(self):
        return [FIRST_INTEGRATION]

    def test_catalog_parsing(self, catalogs, project):
        with self.setup_test(catalogs, project):
            run_dbt(["run"])
            write_integration = project.adapter.get_catalog_integration("my_write_integration")
            assert isinstance(write_integration, TestCatalogIntegration)
            assert write_integration.name == "my_write_integration"
            assert write_integration.external_volume == "my_external_volume"
            assert write_integration.table_format == "iceberg"
            assert write_integration.catalog_type == "glue"
            assert write_integration.my_custom_property == "some_value"


class TestMultipleWriteIntegrations(BaseCatalogTest):
    @pytest.fixture
    def integrations(self):
        return [FIRST_INTEGRATION, SECOND_INTEGRATION]


class TestValidActiveWriteIntegration(TestMultipleWriteIntegrations):
    @pytest.fixture
    def active_integration(self):
        return "my_second_write_integration"

    def test_catalog_parsing_adapter_initialialization(self, catalogs, project):
        with self.setup_test(catalogs, project):
            run_dbt(["build"])
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


class TestNoActiveWriteIntegration(TestMultipleWriteIntegrations):
    def test_catalog_parsing_adapter_initialialization(self, catalogs, project):
        with self.setup_test(catalogs, project):
            error_msg = "Catalog 'test_catalog' must specify an 'active_write_integration' when multiple 'write_integrations' are provided."
            with pytest.raises(DbtValidationError, match=error_msg):
                run_dbt(["build"])


class TestInvalidActiveWriteIntegration(TestMultipleWriteIntegrations):
    @pytest.fixture
    def active_integration(self):
        return "my_nonexistent_write_integration"

    def test_catalog_parsing_adapter_initialialization(self, catalogs, project):
        with self.setup_test(catalogs, project):
            error_msg = "must specify an 'active_write_integration' from its set of defined 'write_integrations'"
            with pytest.raises(DbtValidationError, match=error_msg):
                run_dbt(["build"])


class TestMultipleWriteIntegrationsDuplicateName(BaseCatalogTest):
    @pytest.fixture
    def integrations(self):
        return [FIRST_INTEGRATION, FIRST_INTEGRATION]

    def test_catalog_parsing(self, catalogs, project):
        with self.setup_test(catalogs, project):
            with pytest.raises(
                DbtValidationError,
                match="Catalog 'test_catalog' cannot have multiple 'write_integrations' with the same name: my_write_integration",
            ):
                run_dbt(["build"])
