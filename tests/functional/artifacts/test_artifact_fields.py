import pytest
from dbt.tests.util import run_dbt, get_manifest, get_artifact

# This is a place to put specific tests for contents of artifacts that we
# don't want to bother putting in the big artifact output test, which is
# hard to update.

my_model_sql = "select 1 as fun"

schema_yml = """
version: 2
models:
  - name: my_model
    columns:
      - name: fun
        tests:
          - not_null
"""


class TestRelationNameInTests:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "schema.yml": schema_yml,
        }

    def test_relation_name_in_tests(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        test_id = "test.test.not_null_my_model_fun.bf3b032a01"
        assert test_id in manifest.nodes
        assert manifest.nodes[test_id].relation_name is None

        results = run_dbt(["test", "--store-failures"])
        assert len(results) == 1
        # The relation_name for tests with previously generated manifest and
        # store_failures passed in on the command line, will be in the manifest.json
        # but not in the parsed manifest.
        manifest = get_manifest(project.project_root)
        assert manifest.nodes[test_id].relation_name is None
        manifest_json = get_artifact(project.project_root, "target", "manifest.json")
        assert test_id in manifest_json["nodes"]
        relation_name = manifest_json["nodes"][test_id]["relation_name"]
        assert relation_name
        assert '"not_null_my_model_fun"' in relation_name
