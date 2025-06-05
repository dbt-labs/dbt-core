import pytest

from dbt.tests.util import run_dbt

dbt_project_update = """
models:
  my_dbt_project:
    +materialized: table

tests:
  +store_failures: true
"""

foo_sql = """
select 1 as id
"""

schema_yml = """
models:
  - name: foo
    columns:
      - name: id
        tests:
          - unique
"""


class TestGenericTestUnrenderedConfig:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return dbt_project_update

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "foo.sql": foo_sql,
            "schema.yml": schema_yml,
        }

    def test_unrendered_config(self, project):
        manifest = run_dbt(["parse"])
        assert manifest
        test_node_id = "test.test.unique_foo_id.fa8c520a2e"
        test_node = manifest.nodes[test_node_id]
        assert test_node.unrendered_config == {"store_failures": True}
