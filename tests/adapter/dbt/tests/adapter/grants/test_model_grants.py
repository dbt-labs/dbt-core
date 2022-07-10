import pytest
from dbt.tests.util import (
    run_dbt_and_capture,
    get_manifest,
    write_file,
)
from dbt.tests.adapter.grants.base_grants import BaseGrants

my_model_sql = """
  select 1 as fun
"""

model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
"""

user2_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""

table_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
"""

user2_table_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""

multiple_users_table_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}", "{{ env_var('DBT_TEST_USER_2') }}"]
"""

multiple_privileges_table_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      materialized: table
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
        insert: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""


class BaseModelGrants(BaseGrants):
    @pytest.fixture(scope="class")
    def models(self):
        updated_schema = self.interpolate_name_overrides(model_schema_yml)
        return {
            "my_model.sql": my_model_sql,
            "schema.yml": updated_schema,
        }

    def test_view_table_grants(self, project, get_test_users):
        # we want the test to fail, not silently skip
        test_users = get_test_users
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]
        insert_privilege_name = self.privilege_grantee_name_overrides()["insert"]
        assert len(test_users) == 3

        # View materialization, single select grant
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        model = manifest.nodes[model_id]
        expected = {select_privilege_name: [test_users[0]]}
        assert model.config.grants == expected
        assert model.config.materialized == "view"
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # View materialization, change select grant user
        updated_yaml = self.interpolate_name_overrides(user2_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1

        expected = {select_privilege_name: [get_test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # Table materialization, single select grant
        updated_yaml = self.interpolate_name_overrides(table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {select_privilege_name: [test_users[0]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # Table materialization, change select grant user
        updated_yaml = self.interpolate_name_overrides(user2_table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {select_privilege_name: [test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # Table materialization, multiple grantees
        updated_yaml = self.interpolate_name_overrides(multiple_users_table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {select_privilege_name: [test_users[0], test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

        # Table materialization, multiple privileges
        updated_yaml = self.interpolate_name_overrides(multiple_privileges_table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {select_privilege_name: [test_users[0]], insert_privilege_name: [test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)


class TestModelGrants(BaseModelGrants):
    pass
