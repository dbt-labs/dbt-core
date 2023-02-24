import pytest
from dbt.tests.util import run_dbt, get_manifest
from dbt.node_types import AccessType

my_model_sql = "select 1 as fun"

another_model_sql = "select 1234 as notfun"

yet_another_model_sql = "select 999 as weird"

schema_yml = """
version: 2

models:
  - name: my_model
    description: "my model"
    access: public
  - name: another_model
    description: "another model"
    access: unsupported
  - name: yet_another_model
    description: "yet another model"
"""


class TestAccess:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "another_model.sql": another_model_sql,
            "yet_another_model.sql": yet_another_model_sql,
            "schema.yml": schema_yml,
        }

    def test_access_attribute(self, project):

        results = run_dbt(["run"])
        assert len(results) == 3

        manifest = get_manifest(project.project_root)
        my_model_id = "model.test.my_model"
        another_model_id = "model.test.another_model"
        yet_another_model_id = "model.test.yet_another_model"
        assert my_model_id in manifest.nodes
        assert another_model_id in manifest.nodes
        assert yet_another_model_id in manifest.nodes

        assert manifest.nodes[my_model_id].access == AccessType.Public
        assert manifest.nodes[another_model_id].access == AccessType.Protected
        assert manifest.nodes[yet_another_model_id].access == AccessType.Protected
