import re

import pytest

from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import TestNode
from dbt.exceptions import CompilationError
from dbt.tests.util import get_manifest, run_dbt
from tests.functional.schema_tests.fixtures import (
    custom_config_yml,
    mixed_config_yml,
    same_key_error_yml,
    seed_csv,
    table_sql,
    test_custom_color_from_config,
)


def _select_test_node(manifest: Manifest, pattern: re.Pattern[str]):
    # Find the test_id dynamically
    test_id = None
    for node_id in manifest.nodes:
        if pattern.match(node_id):
            test_id = node_id
            break

    # Ensure the test_id was found
    assert test_id is not None, "Test ID matching the pattern was not found in the manifest nodes"
    return manifest.nodes[test_id]


def get_table_persistence(project, table_name):
    sql = f"""
        SELECT
          relpersistence
        FROM pg_class
        WHERE relname like '%{table_name}%'
    """
    result = project.run_sql(sql, fetch="one")
    assert len(result) == 1
    return result[0]


class BaseDataTestsConfig:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seed_csv}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"custom_color_from_config.sql": test_custom_color_from_config}

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
        run_dbt(["run"])
        run_dbt(["test", "--log-level", "debug"], expect_pass=False)

        manifest = get_manifest(project.project_root)
        # Pattern to match the test_id without the specific suffix
        pattern = re.compile(r"test\.test\.accepted_values_table_color__blue__red\.\d+")

        test_node: TestNode = _select_test_node(manifest, pattern)
        # Proceed with the assertions
        assert "custom_config_key" in test_node.config
        assert test_node.config["custom_config_key"] == "some_value"

        # pattern = re.compile(r"test\.test\.custom_color_from_config.*")
        # test_node = _select_test_node(manifest, pattern)
        persistence = get_table_persistence(project, "custom_color_from_config_table_color")

        assert persistence == "u"


class TestMixedDataTestConfig(BaseDataTestsConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql, "mixed_config.yml": mixed_config_yml}

    def test_mixed_config(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)

        # Pattern to match the test_id without the specific suffix
        pattern = re.compile(r"test\.test\.accepted_values_table_color__blue__red\.\d+")

        # Find the test_id dynamically
        test_id = None
        for node_id in manifest.nodes:
            if pattern.match(node_id):
                test_id = node_id
                break

        # Ensure the test_id was found
        assert (
            test_id is not None
        ), "Test ID matching the pattern was not found in the manifest nodes"

        # Proceed with the assertions
        test_node = manifest.nodes[test_id]
        assert "custom_config_key" in test_node.config
        assert test_node.config["custom_config_key"] == "some_value"
        assert "severity" in test_node.config
        assert test_node.config["severity"] == "warn"


class TestSameKeyErrorDataTestConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql, "same_key_error.yml": same_key_error_yml}

    def test_same_key_error(self, project):
        """
        Test that verifies dbt raises a CompilationError when the test configuration
        contains the same key at the top level and inside the config dictionary.
        """
        # Run dbt and expect a CompilationError due to the invalid configuration
        with pytest.raises(CompilationError) as exc_info:
            run_dbt(["parse"])

        # Extract the exception message
        exception_message = str(exc_info.value)

        # Assert that the error message contains the expected text
        assert "Test cannot have the same key at the top-level and in config" in exception_message

        # Assert that the error message contains the context of the error
        assert "models/same_key_error.yml" in exception_message
