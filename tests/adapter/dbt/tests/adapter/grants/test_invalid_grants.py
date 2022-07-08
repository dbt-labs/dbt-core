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

invalid_user_table_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select: ['invalid_user']
"""

invalid_privilege_table_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        my_select: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""


class BaseInvalidGrants(BaseGrants):
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": my_model_sql, "schema.yml": invalid_user_table_model_schema_yml}
    
    def grantee_does_not_exist_error(self):
        return "does not exist"
        
    def privilege_does_not_exist_error(self):
        return "unrecognized privilege"
    
    def test_nonexistent_grantee(self, project, get_test_users, logs_dir):
        # failure when grant to a user/role that doesn't exist
        write_file(
            invalid_user_table_model_schema_yml, project.project_root, "models", "schema.yml"
        )
        (results, log_output) = run_dbt_and_capture(["run"], expect_pass=False)
        assert self.grantee_does_not_exist_error() in log_output

        # failure when grant to a privilege that doesn't exist
        write_file(
            invalid_privilege_table_model_schema_yml, project.project_root, "models", "schema.yml"
        )
        (results, log_output) = run_dbt_and_capture(["run"], expect_pass=False)
        assert self.privilege_does_not_exist_error() in log_output


class TestInvalidGrants(BaseInvalidGrants):
    pass
