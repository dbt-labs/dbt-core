import pytest

from dbt.tests.util import get_manifest, run_dbt
from tests.functional.schema_tests.fixtures import (
    custom_config_yml,
    mixed_config_yml,
    same_key_error_yml,
    seed_csv,
    table_sql,
)


class BaseDataTestsConfig:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        run_dbt(["seed"])


class TestCustomDataTestConfig(BaseDataTestsConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql, "custom_config.yml": custom_config_yml}

    def test_custom_config(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        test_id = "test.test.accepted_values_table_color__blue__red.9482147132"
        assert test_id in manifest.nodes
        test_node = manifest.nodes[test_id]
        assert "custom_config_key" in test_node.config
        assert test_node.config["custom_config_key"] == "some_value"


class TestMixedDataTestConfig(BaseDataTestsConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql, "mixed_config.yml": mixed_config_yml}

    def test_mixed_config(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        test_id = "test.test.accepted_values_table_color__blue__red.9482147132"
        assert test_id in manifest.nodes
        test_node = manifest.nodes[test_id]
        assert "custom_config_key" in test_node.config
        assert test_node.config["custom_config_key"] == "some_value"
        assert "severity" in test_node.config
        assert test_node.config["severity"] == "warn"


class TestSameKeyErrorDataTestConfig(BaseDataTestsConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql, "same_key_error.yml": same_key_error_yml}

    def test_same_key_error(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        test_id = "test.test.accepted_values_table_color__blue__red.9482147132"
        assert test_id in manifest.nodes
        test_node = manifest.nodes[test_id]
        breakpoint()
        assert "severity" in test_node.config
        assert test_node.config["severity"] == "warn"
