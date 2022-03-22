import os
import pytest
from dbt.tests.util import run_dbt
from tests.functional.simple_snapshot.fixtures import (
    models__schema_yml,
    models__ref_snapshot_sql,
    macros__test_no_overlaps_sql,
    macros_custom_snapshot__custom_sql,
    seeds__seed_newcol_csv,
    seeds__seed_csv,
)

NUM_SNAPSHOT_MODELS = 1


snapshots_pg_custom_invalid__snapshot_sql = """
{% snapshot snapshot_actual %}
    {# this custom strategy does not exist  in the 'dbt' package #}
    {{
        config(
            target_database=var('target_database', database),
            target_schema=var('target_schema', schema),
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='dbt.custom',
            updated_at='updated_at',
        )
    }}
    select * from {{target.database}}.{{target.schema}}.seed

{% endsnapshot %}
"""


@pytest.fixture(scope="class")
def snapshots():
    return {"snapshots.sql": snapshots_pg_custom_invalid__snapshot_sql}


@pytest.fixture(scope="class")
def macros():
    return {
        "test_no_overlaps.sql": macros__test_no_overlaps_sql,
        "custom.sql": macros_custom_snapshot__custom_sql,
    }


@pytest.fixture(scope="class")
def models():
    return {
        "schema.yml": models__schema_yml,
        "ref_snapshot.sql": models__ref_snapshot_sql,
    }


@pytest.fixture(scope="class")
def seeds():
    return {"seed_newcol.csv": seeds__seed_newcol_csv, "seed.csv": seeds__seed_csv}


def test_custom_snapshot_invalid_namespace(project):
    path = os.path.join(project.test_data_dir, "seed_pg.sql")
    project.run_sql_file(path)
    results = run_dbt(["snapshot"], expect_pass=False)
    assert len(results) == NUM_SNAPSHOT_MODELS
