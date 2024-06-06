import pytest

from dbt.tests.util import run_dbt

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
