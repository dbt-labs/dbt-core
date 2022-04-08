import pytest
from dbt.tests.util import run_dbt


snapshot_sql = """
{% snapshot check_cols_cycle %}

    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['color']
        )
    }}

    {% if var('version') == 1 %}

        select 1 as id, 'red' as color union all
        select 2 as id, 'green' as color

    {% elif var('version') == 2 %}

        select 1 as id, 'blue' as color union all
        select 2 as id, 'green' as color

    {% elif var('version') == 3 %}

        select 1 as id, 'red' as color union all
        select 2 as id, 'pink' as color

    {% else %}
        {% do exceptions.raise_compiler_error("Got bad version: " ~ var('version')) %}
    {% endif %}

{% endsnapshot %}
"""

snapshot_test_sql = """
with query as (

    -- check that the current value for id=1 is red
    select case when (
        select count(*)
        from {{ ref('check_cols_cycle') }}
        where id = 1 and color = 'red' and dbt_valid_to is null
    ) = 1 then 0 else 1 end as failures

    union all

    -- check that the previous 'red' value for id=1 is invalidated
    select case when (
        select count(*)
        from {{ ref('check_cols_cycle') }}
        where id = 1 and color = 'red' and dbt_valid_to is not null
    ) = 1 then 0 else 1 end as failures

    union all

    -- check that there's only one current record for id=2
    select case when (
        select count(*)
        from {{ ref('check_cols_cycle') }}
        where id = 2 and color = 'pink' and dbt_valid_to is null
    ) = 1 then 0 else 1 end as failures

    union all

    -- check that the previous value for id=2 is represented
    select case when (
        select count(*)
        from {{ ref('check_cols_cycle') }}
        where id = 2 and color = 'green' and dbt_valid_to is not null
    ) = 1 then 0 else 1 end as failures

    union all

    -- check that there are 5 records total in the table
    select case when (
        select count(*)
        from {{ ref('check_cols_cycle') }}
    ) = 5 then 0 else 1 end as failures

)

select *
from query
where failures = 1
"""


@pytest.fixture(scope="class")
def snapshots():
    return {"my_snapshot.sql": snapshot_sql}


@pytest.fixture(scope="class")
def tests():
    return {"my_test.sql": snapshot_test_sql}


def test_simple_snapshot(project):

    results = run_dbt(["snapshot", "--vars", "version: 1"])
    assert len(results) == 1

    results = run_dbt(["snapshot", "--vars", "version: 2"])
    assert len(results) == 1

    results = run_dbt(["snapshot", "--vars", "version: 3"])
    assert len(results) == 1

    run_dbt(["test", "--select", "test_type:singular", "--vars", "version: 3"])
