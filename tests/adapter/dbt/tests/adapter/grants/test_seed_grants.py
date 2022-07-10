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
    def seeds_support_partial_refresh(self):
        return True

    @pytest.fixture(scope="class")
    def seeds(self):
        updated_schema = self.interpolate_name_overrides(schema_base_yml)
        return {
            "my_seed.csv": seeds__my_seed_csv,
            "schema.yml": updated_schema,
        }

    def test_seed_grants(self, project, get_test_users):
        test_users = get_test_users
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]

        # seed command
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        seed_id = "seed.test.my_seed"
        seed = manifest.nodes[seed_id]
        expected = {select_privilege_name: [test_users[0]]}
        assert seed.config.grants == expected
        assert "grant " in log_output
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # run it again, with no config changes
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        if self.seeds_support_partial_refresh():
            # grants carried over -- nothing should have changed
            assert "revoke " not in log_output
            assert "grant " not in log_output
        else:
            # seeds are always full-refreshed on this adapter, so we need to re-grant
            assert "grant " in log_output
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # change the grantee, assert it updates
        updated_yaml = self.interpolate_name_overrides(user2_schema_base_yml)
        write_file(updated_yaml, project.project_root, "seeds", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        expected = {select_privilege_name: [test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # run it again, with --full-refresh, grants should be the same
        run_dbt(["seed", "--full-refresh"])
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # change config to 'grants: {}' -- should be completely ignored
        updated_yaml = self.interpolate_name_overrides(ignore_grants_yml)
        write_file(updated_yaml, project.project_root, "seeds", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        assert "revoke " not in log_output
        assert "grant " not in log_output
        manifest = get_manifest(project.project_root)
        seed_id = "seed.test.my_seed"
        seed = manifest.nodes[seed_id]
        expected_config = {}
        expected_actual = {select_privilege_name: [test_users[1]]}
        assert seed.config.grants == expected_config
        if self.seeds_support_partial_refresh():
            # ACTUAL grants will NOT match expected grants
            self.assert_expected_grants_match_actual(project, "my_seed", expected_actual)
        else:
            # there should be ZERO grants on the seed
            self.assert_expected_grants_match_actual(project, "my_seed", expected_config)

        # now run with ZERO grants -- all grants should be removed
        # whether explicitly (revoke) or implicitly (recreated without any grants added on)
        updated_yaml = self.interpolate_name_overrides(zero_grants_yml)
        write_file(updated_yaml, project.project_root, "seeds", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        if self.seeds_support_partial_refresh():
            assert "revoke " in log_output
        expected = {}
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # run it again -- dbt shouldn't try to grant or revoke anything
        (results, log_output) = run_dbt_and_capture(["--debug", "seed"])
        assert len(results) == 1
        assert "revoke " not in log_output
        assert "grant " not in log_output
        self.assert_expected_grants_match_actual(project, "my_seed", expected)


class TestSeedGrants(BaseSeedGrants):
    pass
