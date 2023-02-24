import pytest
from dbt.tests.util import run_dbt, get_manifest, write_file, rm_file
from dbt.node_types import AccessType
from dbt.exceptions import InvalidAccessTypeError, DbtReferenceError

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
    description: "yet another model"
"""

v2_schema_yml = """
version: 2

models:
  - name: my_model
    description: "my model"
    access: public
  - name: another_model
    description: "another model"
  - name: yet_another_model
    description: "yet another model"
    access: unsupported
"""

ref_my_model_sql = """
   select fun from {{ ref('my_model') }}
"""

v3_schema_yml = """
version: 2

groups:
  - name: analytics
    owner:
      name: analytics_owner
  - name: marts
    owner:
      name: marts_owner

models:
  - name: my_model
    description: "my model"
    group: analytics
    access: private
  - name: another_model
    description: "yet another model"
  - name: ref_my_model
    description: "a model that refs my_model"
    group: analytics
"""

v4_schema_yml = """
version: 2

models:
  - name: my_model
    description: "my model"
    group: analytics
    access: private
  - name: another_model
    description: "yet another model"
  - name: ref_my_model
    description: "a model that refs my_model"
    group: marts
"""


class TestAccess:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "another_model.sql": yet_another_model_sql,
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

        # write a file with an invalid access value
        write_file(yet_another_model_sql, project.project_root, "models", "yet_another_model.sql")
        write_file(v2_schema_yml, project.project_root, "models", "schema.yml")

        with pytest.raises(InvalidAccessTypeError):
            run_dbt(["run"])

        # Remove invalid access files and write out model that refs my_model
        rm_file(project.project_root, "models", "yet_another_model.sql")
        write_file(schema_yml, project.project_root, "models", "schema.yml")
        write_file(ref_my_model_sql, project.project_root, "models", "ref_my_model.sql")
        results = run_dbt(["run"])
        assert len(results) == 3

        # make my_model private, set same group on my_model and ref_my_model
        write_file(v3_schema_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["run"])
        assert len(results) == 3

        # Change group on ref_my_model and it should raise
        write_file(v4_schema_yml, project.project_root, "models", "schema.yml")
        with pytest.raises(DbtReferenceError):
            run_dbt(["run"])
