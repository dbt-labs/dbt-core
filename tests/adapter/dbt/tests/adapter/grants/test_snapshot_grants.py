import pytest
from dbt.tests.util import (
    run_dbt,
    run_dbt_and_capture,
    get_manifest,
    relation_from_name,
    write_file,
)
from dbt.tests.adapter.grants.base_grants import BaseGrants

my_snapshot_sql = """
{% snapshot my_snapshot %}
    {{ config(
        check_cols='all', unique_key='id', strategy='check',
        target_database=database, target_schema=schema
    ) }}
    select 1 as id, 'blue' as color
{% endsnapshot %}
""".strip()

snapshot_schema_yml = """
version: 2
snapshots:
  - name: my_snapshot
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
"""

user2_snapshot_schema_yml = """
version: 2
snapshots:
  - name: my_snapshot
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""


class BaseSnapshotGrants(BaseGrants):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "my_snapshot.sql": my_snapshot_sql,
            "schema.yml": snapshot_schema_yml
        }

    def test_snapshot_grants(self, project, get_test_users):
        test_users = get_test_users
        
        # run the snapshot
        results = run_dbt(["snapshot"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        snapshot_id = "snapshot.test.my_snapshot"
        snapshot = manifest.nodes[snapshot_id]
        expected = {"select": [test_users[0]]}
        assert snapshot.config.grants == expected
        self.assert_expected_grants_match_actual(project, "my_snapshot", expected)

        # run it again, nothing should have changed
        (results, log_output) = run_dbt_and_capture(["snapshot"])
        assert len(results) == 1
        assert "revoke select" not in log_output
        assert "grant select" not in log_output
        self.assert_expected_grants_match_actual(project, "my_snapshot", expected)
        
        # change the grantee, assert it updates
        write_file(user2_snapshot_schema_yml, project.project_root, "snapshots", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["snapshot"])
        assert len(results) == 1
        expected = {"select": [test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_snapshot", expected)


class TestSnapshotGrants(BaseSnapshotGrants):
    pass
