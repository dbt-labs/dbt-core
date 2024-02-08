import pytest
from dbt.tests.util import run_dbt, rm_file, write_file, get_manifest


schema_yml = """
models:
  - name: foo
    config:
      materialized: table
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [id, user_name]
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
      - name: user_name
        data_type: text
"""

foo_sql = """
select 1 as id, 'alice' as user_name
"""

versioned_schema_yml = """
models:
  - name: foo
    latest_version: 1
    config:
      materialized: table
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [id, user_name]
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
      - name: user_name
        data_type: text
    versions:
      - v: 1
"""


class TestVersionedModelConstraints:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "foo.sql": foo_sql,
            "schema.yml": schema_yml,
        }

    def test_versioned_model_constraints(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_node = manifest.nodes["model.test.foo"]
        assert len(model_node.constraints) == 1

        # remove foo.sql and create foo_v1.sql
        rm_file(project.project_root, "models", "foo.sql")
        write_file(foo_sql, project.project_root, "models", "foo_v1.sql")
        write_file(versioned_schema_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["run"])
        assert len(results) == 1

        manifest = get_manifest(project.project_root)
        model_node = manifest.nodes["model.test.foo.v1"]
        assert model_node.contract.enforced is True
        assert len(model_node.constraints) == 1
