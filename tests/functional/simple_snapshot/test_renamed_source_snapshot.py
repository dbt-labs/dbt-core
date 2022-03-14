import pytest
from dbt.tests.util import run_dbt
from tests.functional.simple_snapshot.fixtures import (
    seeds__seed_newcol_csv,
    seeds__seed_csv,
    macros__test_no_overlaps_sql,
    macros_custom_snapshot__custom_sql,
)


snapshots_checkall__snapshot_sql = """
{% snapshot my_snapshot %}
    {{ config(check_cols='all', unique_key='id', strategy='check', target_database=database, target_schema=schema) }}
    select * from {{ ref(var('seed_name', 'seed')) }}
{% endsnapshot %}
"""


@pytest.fixture(scope="class")
def snapshots():
    return {"snapshot.sql": snapshots_checkall__snapshot_sql}


@pytest.fixture(scope="class")
def macros():
    return {
        "test_no_overlaps.sql": macros__test_no_overlaps_sql,
        "custom.sql": macros_custom_snapshot__custom_sql,
    }


@pytest.fixture(scope="class")
def seeds():
    return {"seed_newcol.csv": seeds__seed_newcol_csv, "seed.csv": seeds__seed_csv}


def test_renamed_source(project):
    run_dbt(["seed"])
    run_dbt(["snapshot"])
    database = project.database
    results = project.run_sql(
        "select * from {}.{}.my_snapshot".format(database, project.test_schema),
        fetch="all",
    )
    assert len(results) == 3
    for result in results:
        assert len(result) == 6

    # over ride the ref var in the snapshot definition to use a seed with an additional column, last_name
    run_dbt(["snapshot", "--vars", "{seed_name: seed_newcol}"])
    results = project.run_sql(
        "select * from {}.{}.my_snapshot where last_name is not NULL".format(
            database, project.test_schema
        ),
        fetch="all",
    )
    assert len(results) == 3

    for result in results:
        # new column
        assert len(result) == 7
        assert result[-1] is not None

    results = project.run_sql(
        "select * from {}.{}.my_snapshot where last_name is NULL".format(
            database, project.test_schema
        ),
        fetch="all",
    )
    assert len(results) == 3
    for result in results:
        # new column
        assert len(result) == 7
