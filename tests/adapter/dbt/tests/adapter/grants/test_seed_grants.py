import pytest
from dbt.tests.util import (
    run_dbt,
    run_dbt_and_capture,
    get_manifest,
    relation_from_name,
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


class BaseSeedGrants(BaseGrants):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "my_seed.csv": seeds__my_seed_csv,
            "schema.yml": schema_base_yml
        }

    def test_seed_grants(self, project, get_test_users):
        test_users = get_test_users
        
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        seed_id = "seed.test.my_seed"
        seed = manifest.nodes[seed_id]
        expected = {"select": [test_users[0]]}
        assert seed.config.grants == expected
        self.assert_expected_grants_match_actual(project, "my_seed", expected)

        # run it again, nothing should have changed
        (results, log_output) = run_dbt_and_capture(["seed"])
        assert len(results) == 1
        assert "revoke select" not in log_output
        assert "grant select" not in log_output
        self.assert_expected_grants_match_actual(project, "my_seed", expected)
        
        # change the grantee, assert it updates
        write_file(user2_schema_base_yml, project.project_root, "seeds", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["seed"])
        assert len(results) == 1
        expected = {"select": [test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_seed", expected)


class TestSeedGrants(BaseSeedGrants):
    pass
