import pytest
from dbt.tests.util import run_dbt
from tests.functional.simple_snapshot.fixtures import models_slow__gen_sql

snapshots_slow__snapshot_sql = """

{% snapshot my_slow_snapshot %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id',
            strategy='timestamp',
            updated_at='updated_at'
        )
    }}

    select
        id,
        updated_at,
        seconds

    from {{ ref('gen') }}

{% endsnapshot %}
"""


test_snapshots_slow__test_timestamps_sql = """

/*
    Assert that the dbt_valid_from of the latest record
    is equal to the dbt_valid_to of the previous record
*/

with snapshot as (

    select * from {{ ref('my_slow_snapshot') }}

)

select
    snap1.id,
    snap1.dbt_valid_from as new_valid_from,
    snap2.dbt_valid_from as old_valid_from,
    snap2.dbt_valid_to as old_valid_to

from snapshot as snap1
join snapshot as snap2 on snap1.id = snap2.id
where snap1.dbt_valid_to is null
  and snap2.dbt_valid_to is not null
  and snap1.dbt_valid_from != snap2.dbt_valid_to
"""


@pytest.fixture(scope="class")
def models():
    return {"gen.sql": models_slow__gen_sql}


@pytest.fixture(scope="class")
def snapshots():
    return {"snapshot.sql": snapshots_slow__snapshot_sql}


@pytest.fixture(scope="class")
def tests():
    return {"test_timestamps.sql": test_snapshots_slow__test_timestamps_sql}


def test_slow(project):
    results = run_dbt(["snapshot"])
    assert len(results) == 1

    results = run_dbt(["snapshot"])
    assert len(results) == 1

    results = run_dbt(["test"])
    assert len(results) == 1
