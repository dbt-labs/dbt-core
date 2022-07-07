import pytest
import os
from dbt.tests.util import (
    run_dbt,
    run_dbt_and_capture,
    get_manifest,
    read_file,
    relation_from_name,
    rm_file,
    write_file,
)

TEST_USER_ENV_VARS = ["DBT_TEST_USER_1", "DBT_TEST_USER_2", "DBT_TEST_USER_3"]

my_model_sql = """
  select 1 as fun
"""

model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      grants:
        select: ["dbt_test_user_1"]
"""

user2_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      grants:
        select: ["dbt_test_user_2"]
"""

class TestModelGrants:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": my_model_sql, "schema.yml": model_schema_yml}

    @pytest.fixture(scope="class", autouse=True)
    def get_test_users(self, project):
        test_users = []
        for env_var in TEST_USER_ENV_VARS:
            user_name = os.getenv(env_var)
            if user_name:
                test_users.append(user_name)
        return test_users

    def test_basic(self, project, get_test_users, logs_dir):
        # we want the test to fail, not silently skip
        test_users = get_test_users
        assert len(test_users) == 3

        # Tests a project with a single model, view materialization
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        model = manifest.nodes[model_id]
        expected = {"select": [test_users[0]]}
        assert model.config.grants == expected
        assert model.config.materialized == "view"

        my_model_relation = relation_from_name(project.adapter, "my_model")
        grant_log_line = f"grant select on table {my_model_relation} to {test_users[0]};"
        assert grant_log_line in log_output

        # Switch to a different user, still view materialization
        write_file(user2_model_schema_yml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        log_output = read_file(logs_dir, "dbt.log")
        grant_log_line = f"grant select on table {my_model_relation} to {test_users[1]};"
        assert grant_log_line in log_output
        assert "revoke select" not in log_output


