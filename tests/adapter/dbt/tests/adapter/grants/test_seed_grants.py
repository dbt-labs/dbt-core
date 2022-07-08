import pytest
from dbt.tests.util import (
    run_dbt,
    run_dbt_and_capture,
    get_manifest,
    write_file,
)
from dbt.tests.adapter.grants.base_grants import BaseGrants

seeds__my_seed_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
""".lstrip()

schema_base_yml = """
version: 2
seeds:
  - name: my_seed
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
"""

user2_schema_base_yml = """
version: 2
seeds:
  - name: my_seed
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""

ignore_grants_yml = """
version: 2
seeds:
  - name: my_seed
    config:
      grants: {}
"""

zero_grants_yml = """
version: 2
seeds:
  - name: my_seed
    config:
      grants:
        select: []
"""


class BaseSeedGrants(BaseGrants):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "my_seed.csv": seeds__my_seed_csv,
            "schema.yml": self.interpolate_privilege_names(schema_base_yml),
        }

    def test_seed_grants(self, project, get_test_users):
        test_users = get_test_users
        select_privilege_name = self.privilege_names()["select"]

        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        seed_id = "seed.test.my_seed"
        seed = manifest.nodes[seed_id]
        expected = {select_privilege_name: [test_users[0]]}
        assert seed.config.grants == expected
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # run it again, nothing should have changed
        (results, log_output) = run_dbt_and_capture(["seed"])
        assert len(results) == 1
        assert "revoke " not in log_output
        assert "grant " not in log_output
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # change the grantee, assert it updates
        updated_yaml = self.interpolate_privilege_names(user2_schema_base_yml)
        write_file(updated_yaml, project.project_root, "seeds", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["seed"])
        assert len(results) == 1
        expected = {select_privilege_name: [test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # change config to 'grants: {}' -- should be completely ignored
        updated_yaml = self.interpolate_privilege_names(ignore_grants_yml)
        write_file(updated_yaml, project.project_root, "seeds", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["seed"])
        assert len(results) == 1
        assert "revoke " not in log_output
        assert "grant " not in log_output
        manifest = get_manifest(project.project_root)
        seed_id = "seed.test.my_seed"
        seed = manifest.nodes[seed_id]
        expected_config = {}
        expected_actual = {select_privilege_name: [test_users[1]]}
        assert seed.config.grants == expected_config
        # ACTUAL grants will NOT match expected grants
        self.assert_expected_grants_match_actual(project, "my_seed", expected_actual)

        # now run with ZERO grants -- all grants should be removed
        updated_yaml = self.interpolate_privilege_names(zero_grants_yml)
        write_file(updated_yaml, project.project_root, "seeds", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["seed"])
        assert len(results) == 1
        assert "revoke " in log_output
        expected = {}
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # run it again -- dbt shouldn't try to grant or revoke anything
        (results, log_output) = run_dbt_and_capture(["seed"])
        assert len(results) == 1
        assert "revoke " not in log_output
        assert "grant " not in log_output
        self.assert_expected_grants_match_actual(project, "my_seed", expected)


class TestSeedGrants(BaseSeedGrants):
    pass
