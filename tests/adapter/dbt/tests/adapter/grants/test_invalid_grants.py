import pytest
from dbt.tests.util import (
    run_dbt_and_capture,
    write_file,
)
from dbt.tests.adapter.grants.base_grants import BaseGrants

my_invalid_model_sql = """
  select 1 as fun
"""

invalid_user_table_model_schema_yml = """
version: 2
models:
  - name: my_invalid_model
    config:
      materialized: table
      grants:
        select: ['invalid_user']
"""

invalid_privilege_table_model_schema_yml = """
version: 2
models:
  - name: my_invalid_model
    config:
      materialized: table
      grants:
        fake_privilege: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""


class BaseInvalidGrants(BaseGrants):
    # The purpose of this test is to understand the user experience when providing
    # an invalid 'grants' configuration. dbt will *not* try to intercept or interpret
    # the database's own error at runtime -- it will just return those error messages.
    # Hopefully they're helpful!

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_invalid_model.sql": my_invalid_model_sql,
        }

    # Adapters will need to reimplement these methods with the specific
    # language of their database
    def grantee_does_not_exist_error(self):
        return "does not exist"

    def privilege_does_not_exist_error(self):
        return "unrecognized privilege"

    def test_invalid_grants(self, project, get_test_users, logs_dir):
        # failure when grant to a user/role that doesn't exist
        yaml_file = self.interpolate_name_overrides(invalid_user_table_model_schema_yml)
        write_file(yaml_file, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"], expect_pass=False)
        assert self.grantee_does_not_exist_error() in log_output

        # failure when grant to a privilege that doesn't exist
        yaml_file = self.interpolate_name_overrides(invalid_privilege_table_model_schema_yml)
        write_file(yaml_file, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"], expect_pass=False)
        assert self.privilege_does_not_exist_error() in log_output


class TestInvalidGrants(BaseInvalidGrants):
    pass
