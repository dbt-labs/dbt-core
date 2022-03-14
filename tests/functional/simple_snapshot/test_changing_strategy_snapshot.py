import pytest
from dbt.tests.util import run_dbt
from tests.functional.simple_snapshot.fixtures import models_slow__gen_sql


test_snapshots_changing_strategy__test_snapshot_sql = """

{# /*
    Given the repro case for the snapshot build, we'd
    expect to see both records have color='pink'
    in their most recent rows.
*/ #}

with expected as (

    select 1 as id, 'pink' as color union all
    select 2 as id, 'pink' as color

),

actual as (

    select id, color
    from {{ ref('my_snapshot') }}
    where color = 'pink'
      and dbt_valid_to is null

)

select * from expected
except
select * from actual

union all

select * from actual
except
select * from expected
"""


snapshots_changing_strategy__snapshot_sql = """

{#
    REPRO:
        1. Run with check strategy
        2. Add a new ts column and run with check strategy
        3. Run with timestamp strategy on new ts column

        Expect: new entry is added for changed rows in (3)
#}


{% snapshot my_snapshot %}

    {#--------------- Configuration ------------ #}

    {{ config(
        target_schema=schema,
        unique_key='id'
    ) }}

    {% if var('strategy') == 'timestamp' %}
        {{ config(strategy='timestamp', updated_at='updated_at') }}
    {% else %}
        {{ config(strategy='check', check_cols=['color']) }}
    {% endif %}

    {#--------------- Test setup ------------ #}

    {% if var('step') == 1 %}

        select 1 as id, 'blue' as color
        union all
        select 2 as id, 'red' as color

    {% elif var('step') == 2 %}

        -- change id=1 color from blue to green
        -- id=2 is unchanged when using the check strategy
        select 1 as id, 'green' as color, '2020-01-01'::date as updated_at
        union all
        select 2 as id, 'red' as color, '2020-01-01'::date as updated_at

    {% elif var('step') == 3 %}

        -- bump timestamp for both records. Expect that after this runs
        -- using the timestamp strategy, both ids should have the color
        -- 'pink' in the database. This should be in the future b/c we're
        -- going to compare to the check timestamp, which will be _now_
        select 1 as id, 'pink' as color, (now() + interval '1 day')::date as updated_at
        union all
        select 2 as id, 'pink' as color, (now() + interval '1 day')::date as updated_at

    {% endif %}

{% endsnapshot %}
"""


@pytest.fixture(scope="class")
def models():
    return {"gen.sql": models_slow__gen_sql}


@pytest.fixture(scope="class")
def snapshots():
    return {"snapshot.sql": snapshots_changing_strategy__snapshot_sql}


@pytest.fixture(scope="class")
def tests():
    return {"test_snapshot.sql": test_snapshots_changing_strategy__test_snapshot_sql}


def test_changing_strategy(project):
    results = run_dbt(["snapshot", "--vars", "{strategy: check, step: 1}"])
    assert len(results) == 1

    results = run_dbt(["snapshot", "--vars", "{strategy: check, step: 2}"])
    assert len(results) == 1

    results = run_dbt(["snapshot", "--vars", "{strategy: timestamp, step: 3}"])
    assert len(results) == 1

    results = run_dbt(["test"])
    assert len(results) == 1
