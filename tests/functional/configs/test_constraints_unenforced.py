import pytest

from dbt.tests.util import get_manifest, run_dbt

my_model_sql = """
select 1 as id, 'blue' as color
"""

schema_enforced_false_yml = """
models:
  - name: my_model
    config:
      contract:
        enforced: false
    constraints:
      - type: primary_key
        columns: [id]
    columns:
      - name: id
        data_type: integer
      - name: color
        data_type: string
"""

schema_no_contract_yml = """
models:
  - name: my_model
    constraints:
      - type: primary_key
        columns: [id]
    columns:
      - name: id
        data_type: integer
      - name: color
        data_type: string
"""

schema_no_columns_yml = """
models:
  - name: my_model
    constraints:
      - type: primary_key
        columns: [id]
"""


class TestModelConstraintsEnforcedFalse:
    """model['constraints'] is populated even when contract.enforced is false."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "schema.yml": schema_enforced_false_yml,
        }

    def test_constraints_populated(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        node = manifest.nodes["model.test.my_model"]
        assert node.contract.enforced is False
        assert len(node.constraints) == 1
        assert node.constraints[0].columns == ["id"]


class TestModelConstraintsNoContract:
    """model['constraints'] is populated when no contract config is set (defaults to unenforced)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "schema.yml": schema_no_contract_yml,
        }

    def test_constraints_populated(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        node = manifest.nodes["model.test.my_model"]
        assert node.contract.enforced is False
        assert len(node.constraints) == 1
        assert node.constraints[0].columns == ["id"]


class TestModelConstraintsNoColumns:
    """model-level constraints can be defined without specifying columns when unenforced."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "schema.yml": schema_no_columns_yml,
        }

    def test_constraints_populated_without_columns(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        node = manifest.nodes["model.test.my_model"]
        assert node.contract.enforced is False
        assert len(node.constraints) == 1
        assert node.constraints[0].columns == ["id"]
