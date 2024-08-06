import pytest

from dbt.tests.util import run_dbt, write_file

orders_sql = """
select 1 as id, 101 as user_id, 'pending' as status
"""

snapshot_sql = """
{% snapshot orders_snapshot %}

{{
    config(
      target_schema=schema,
      strategy='check',
      unique_key='id',
      check_cols=['status'],
    )
}}

select * from {{ ref('orders') }}

{% endsnapshot %}
"""

snapshot_no_config_sql = """
{% snapshot orders_snapshot %}

select * from {{ ref('orders') }}

{% endsnapshot %}
"""

snapshot_schema_yml = """
snapshots:
  - name: orders_snapshot
    config:
      target_schema: test
      strategy: check
      unique_key: id
      check_cols: ['status']
"""


class TestSnapshotConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {"orders.sql": orders_sql}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot_orders.sql": snapshot_sql}

    def test_config(self, project):
        run_dbt(["run"])
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # try to parse with config in schema file
        write_file(
            snapshot_no_config_sql, project.project_root, "snapshots", "snapshot_orders.sql"
        )
        write_file(snapshot_schema_yml, project.project_root, "snapshots", "snapshot.yml")
        results = run_dbt(["parse"])

        results = run_dbt(["snapshot"])
        assert len(results) == 1
