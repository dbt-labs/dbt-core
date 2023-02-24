import pytest
from dbt.tests.util import run_dbt, get_manifest
from dbt.node_types import AccessType

my_model_sql = "select 1 as fun"

another_model_sql = "select 1234 as notfun"

schema_yml = """
version: 2

models:
  - name: my_model
    description: "my model"
    access: public
  - name: another_model
    description: "another model"
    access: unsupported
"""


class TestAccess:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "another_model.sql": another_model_sql,
            "schema.yml": schema_yml,
        }

    def test_access_attribute(self, project):

        results = run_dbt(["run"])
        assert len(results) == 2

        manifest = get_manifest(project.project_root)
        my_model_id = "model.test.my_model"
        another_model_id = "model.test.another_model"
        assert my_model_id in manifest.nodes
        assert another_model_id in manifest.nodes

        assert manifest.nodes[my_model_id].access == AccessType.Public
        assert manifest.nodes[another_model_id].access == AccessType.Protected
