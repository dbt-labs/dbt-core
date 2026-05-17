import re

import pytest

from dbt.contracts.graph.nodes import TestNode
from dbt.events.types import CustomKeyInConfigDeprecation
from dbt.tests.util import get_manifest, run_dbt
from dbt_common.events.event_catcher import EventCatcher

SQL_HEADER_MARKER = "-- SQL_HEADER_TEST_MARKER"

seed_csv = """
id,value
1,10
2,20
3,30
""".strip()

table_sql = """
select * from {{ ref('seed') }}
"""

# Singular data test using set_sql_header macro
singular_test_with_sql_header = """
{{ config(store_failures_as="ephemeral") }}

{% call set_sql_header(config) %}
  -- SQL_HEADER_TEST_MARKER
{% endcall %}

select id from {{ ref('table') }} where id > 100
"""

# Schema YAML with generic test using sql_header config
generic_test_with_sql_header_yml = """
models:
  - name: table
    columns:
      - name: id
        data_tests:
          - not_null:
              name: generic_test_with_sql_header
              config:
                sql_header: "-- SQL_HEADER_TEST_MARKER"
"""


class TestSingularDataTestSqlHeader:
    """Test that singular data tests properly parse and store sql_header from set_sql_header macro."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"require_sql_header_in_test_configs": True}}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"table.sql": table_sql}

    @pytest.fixture(scope="class")
    def tests(self):
        return {"singular_test_with_sql_header.sql": singular_test_with_sql_header}

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        run_dbt(["seed"])

    def test_singular_test_sql_header_in_config(self, project):
        run_dbt(["run"])
        run_dbt(["test"])

        manifest = get_manifest(project.project_root)
        pattern = re.compile(r"test\.test\.singular_test_with_sql_header")
        test_node = None
        for node_id, node in manifest.nodes.items():
            if pattern.search(node_id) and isinstance(node, TestNode):
                test_node = node
                break

        assert test_node is not None, "Singular test node not found in manifest"
        sql_header = test_node.config.get("sql_header")
        assert sql_header is not None, "sql_header not found in singular test config"
        assert SQL_HEADER_MARKER in sql_header


class TestGenericDataTestSqlHeader:
    """Test that generic data tests properly parse and store sql_header from YAML config."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"require_sql_header_in_test_configs": True}}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table.sql": table_sql,
            "schema.yml": generic_test_with_sql_header_yml,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        run_dbt(["seed"])

    def test_generic_test_sql_header_in_config(self, project):
        run_dbt(["run"])
        run_dbt(["test"])

        manifest = get_manifest(project.project_root)
        pattern = re.compile(r"test\.test\.generic_test_with_sql_header")
        test_node = None
        for node_id, node in manifest.nodes.items():
            if pattern.search(node_id) and isinstance(node, TestNode):
                test_node = node
                break

        assert test_node is not None, "Generic test node not found in manifest"
        assert test_node.config.sql_header == SQL_HEADER_MARKER


class TestGenericDataTestSqlHeaderDeprecation:
    """Test that sql_header in test config emits deprecation warning when flag is off."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table.sql": table_sql,
            "schema.yml": generic_test_with_sql_header_yml,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        run_dbt(["seed"])

    def test_sql_header_deprecation_warning(self, project):
        event_catcher = EventCatcher(CustomKeyInConfigDeprecation)
        run_dbt(
            ["parse", "--no-partial-parse", "--show-all-deprecations"],
            callbacks=[event_catcher.catch],
        )
        assert len(event_catcher.caught_events) == 1
        assert (
            "Custom key `sql_header` found in `config`" in event_catcher.caught_events[0].info.msg
        )

        # Verify sql_header is preserved in the manifest
        manifest = get_manifest(project.project_root)
        pattern = re.compile(r"test\.test\.generic_test_with_sql_header")
        test_node = None
        for node_id, node in manifest.nodes.items():
            if pattern.search(node_id) and isinstance(node, TestNode):
                test_node = node
                break

        assert test_node is not None, "Generic test node not found in manifest"
        assert test_node.config.sql_header == SQL_HEADER_MARKER
