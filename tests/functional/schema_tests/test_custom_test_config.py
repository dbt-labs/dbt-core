import re

import pytest

from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import TestNode
from dbt.exceptions import CompilationError
from dbt.tests.util import get_manifest, run_dbt, update_config_file

custom_config_yml = """
models:
  - name: table
    columns:
      - name: color
        data_tests:
          - accepted_values:
              values: ['blue', 'red']
              config:
                custom_config_key: some_value
          - custom_color_from_config:
              severity: error
              config:
                test_color: orange
                store_failures: true
                unlogged: True
          - not_null:
              config:
                not_null_key: abc
"""

mixed_config_yml = """
models:
  - name: table
    columns:
      - name: color
        data_tests:
          - accepted_values:
              values: ['blue', 'red']
              config:
                custom_config_key: some_value
                severity: warn
          - custom_color_from_config:
              severity: error
              config:
                test_color: blue
"""

same_key_error_yml = """
models:
  - name: table
    columns:
      - name: color
        data_tests:
          - accepted_values:
              values: ['blue', 'red']
              severity: warn
              config:
                severity: error
"""

seed_csv = """
id,color,value
1,blue,10
2,red,20
3,green,30
4,yellow,40
5,blue,50
6,red,60
7,blue,70
8,green,80
9,yellow,90
10,blue,100
""".strip()

table_sql = """
-- content of the table.sql
select * from {{ ref('seed') }}
"""

test_custom_color_from_config = """
{% test custom_color_from_config(model, column_name) %}
    select * from {{ model }}
    where color = '{{ config.get('test_color') }}'
{% endtest %}
"""


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

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        run_dbt(["seed"])


class TestCustomDataTestConfig(BaseDataTestsConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql, "custom_config.yml": custom_config_yml}

    def test_custom_config(self, project):
        run_dbt(["run"])
        run_dbt(["test"], expect_pass=False)

        manifest = get_manifest(project.project_root)
        # Pattern to match the test_id without the specific suffix
        pattern = re.compile(r"test\.test\.accepted_values_table_color__blue__red\.\d+")

        test_node: TestNode = _select_test_node(manifest, pattern)
        # Proceed with the assertions
        assert "custom_config_key" in test_node.config
        assert test_node.config["custom_config_key"] == "some_value"

        custom_color_pattern = re.compile(r"test\.test\.custom_color_from_config.*")
        custom_color_test_node = _select_test_node(manifest, custom_color_pattern)
        assert custom_color_test_node.config.get("test_color") == "orange"
        assert custom_color_test_node.config.get("unlogged") is True
        persistence = get_table_persistence(project, "custom_color_from_config_table_color")
        assert persistence == "u"

        not_null_pattern = re.compile(r"test\.test\.not_null.*")
        not_null_test_node = _select_test_node(manifest, not_null_pattern)
        assert not_null_test_node.config.get("not_null_key") == "abc"

        # set dbt_project.yml config and ensure that schema configs override project configs
        config_patch = {
            "data_tests": {"test_color": "blue", "some_key": "strange", "not_null_key": "def"}
        }
        update_config_file(config_patch, project.project_root, "dbt_project.yml")
        manifest = run_dbt(["parse"])
        custom_color_test_node = _select_test_node(manifest, custom_color_pattern)
        assert custom_color_test_node.config.get("test_color") == "orange"
        assert custom_color_test_node.config.get("some_key") == "strange"
        not_null_test_node = _select_test_node(manifest, not_null_pattern)
        assert not_null_test_node.config.get("not_null_key") == "abc"


class TestMixedDataTestConfig(BaseDataTestsConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql, "mixed_config.yml": mixed_config_yml}

    def test_mixed_config(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)

        # Pattern to match the test_id without the specific suffix
        pattern = re.compile(r"test\.test\.accepted_values_table_color__blue__red\.\d+")
        test_node = _select_test_node(manifest, pattern)

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
