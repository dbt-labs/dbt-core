import pytest
from dbt.tests.tables import TableComparison
from dbt.tests.util import run_dbt, read_file

from tests.functional.basic.test_simple_copy import (
    advanced_incremental_sql,
    compound_sort_sql,
    disabled_sql,
    empty_sql,
    incremental_sql,
    interleaved_sort_sql,
    materialized_sql,
    schema_yml,
    view_model_sql,
)

get_and_ref_sql = """
{%- do adapter.get_relation(database=target.database, schema=target.schema, identifier='MATERIALIZED') -%}

select * from {{ ref('MATERIALIZED') }}
"""


@pytest.fixture(scope="class")
def dbt_profile_data(unique_schema):
    return {
        "config": {"send_anonymous_usage_stats": False},
        "test": {
            "outputs": {
                "default": {
                    "type": "postgres",
                    "threads": 4,
                    "host": "localhost",
                    "port": 5432,
                    "user": "root",
                    "pass": "password",
                    "dbname": "dbtMixedCase",
                    "schema": unique_schema,
                },
            },
            "target": "default",
        },
    }


@pytest.fixture(scope="class")
def models():
    return {
        "ADVANCED_INCREMENTAL.sql": advanced_incremental_sql,
        "COMPOUND_SORT.sql": compound_sort_sql,
        "DISABLED.sql": disabled_sql,
        "EMPTY.sql": empty_sql,
        "GET_AND_REF.sql": get_and_ref_sql,
        "INCREMENTAL.sql": incremental_sql,
        "INTERLEAVED_SORT.sql": interleaved_sort_sql,
        "MATERIALIZED.sql": materialized_sql,
        "SCHEMA.yml": schema_yml,
        "VIEW_MODEL.sql": view_model_sql,
    }


@pytest.fixture(scope="class")
def seeds(test_data_dir):
    # Read seed file and return
    seed_csv = read_file(test_data_dir, "seed-initial.csv")
    return {"seed.csv": seed_csv}


def test_simple_copy_uppercase(project):

    # Load the seed file and check that it worked
    results = run_dbt(["seed"])
    assert len(results) == 1

    # Run the project and ensure that all the models loaded
    results = run_dbt()
    assert len(results) == 7

    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_many_tables_equal(
        ["seed", "VIEW_MODEL", "INCREMENTAL", "MATERIALIZED", "GET_AND_REF"]
    )
