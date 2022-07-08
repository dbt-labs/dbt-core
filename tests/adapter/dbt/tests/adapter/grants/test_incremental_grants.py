import pytest
import os
from dbt.tests.util import (
    run_dbt_and_capture,
    get_manifest,
    relation_from_name,
    write_file,
)
from dbt.tests.adapter.grants.base_grants import BaseGrants

my_model_sql = """
  select 1 as fun
"""

incremental_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      materialized: incremental
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
"""

user2_incremental_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      materialized: incremental
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""

class BaseIncrementalGrants(BaseGrants):
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": my_model_sql, "schema.yml": incremental_model_schema_yml}
            
    def test_incremental_grants(self, project, get_test_users, logs_dir):
        # we want the test to fail, not silently skip
        test_users = get_test_users
        assert len(test_users) == 3
        
        # Incremental materialization, single select grant
        write_file(incremental_model_schema_yml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        #grant_log_line = format_grant_log_line(my_model_relation, test_users[0])
        #assert grant_log_line in log_output
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        model = manifest.nodes[model_id]
        assert model.config.materialized == "incremental"
        expected = {"select": [test_users[0]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # Incremental materialization, run again without changes
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert "revoke select" not in log_output
        assert "grant select" not in log_output
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # Incremental materialization, change select grant user
        write_file(user2_incremental_model_schema_yml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        #grant_log_line = format_grant_log_line(my_model_relation, test_users[1])
        #assert grant_log_line in log_output
        assert "revoke" in log_output
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "incremental"
        expected = {"select": [test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)


class TestIncrementalGrants(BaseIncrementalGrants):
    pass
