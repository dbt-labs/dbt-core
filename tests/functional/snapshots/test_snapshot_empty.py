import pytest

from dbt.tests.util import run_dbt

my_model_sql = """
select 1 as id, {{ dbt.current_timestamp() }} as updated_at
"""

snapshots_yml = """
snapshots:
  - name: my_snapshot
    relation: "ref('my_model')"
    config:
      unique_key: id
      strategy: check
      check_cols: all
      dbt_valid_to_current: "date('9999-12-31')"
"""


class TestSnapshotEmpty:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "snapshots.yml": snapshots_yml,
        }

    def test_check(self, project):
        run_dbt(["run"])
        run_dbt(["snapshot", "--empty"])

        query = "select id, updated_at, dbt_valid_from, dbt_valid_to from {database}.{schema}.my_snapshot order by updated_at asc"
        snapshot_out1 = project.run_sql(query, fetch="all")
        assert snapshot_out1 == []

        run_dbt(["run"])
        run_dbt(["snapshot", "--empty"])
        snapshot_out2 = project.run_sql(query, fetch="all")
        assert snapshot_out2 == []
