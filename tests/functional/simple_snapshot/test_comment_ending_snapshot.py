import pytest
from dbt.tests.util import run_dbt
from tests.functional.simple_snapshot.fixtures import (
    models__schema_yml,
    models__ref_snapshot_sql,
    macros__test_no_overlaps_sql,
)

snapshots_with_comment_at_end__snapshot_sql = """
{% snapshot snapshot_actual %}
    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['email'],
        )
    }}
    select * from {{target.database}}.{{schema}}.seed
    -- Test comment to prevent reccurence of https://github.com/dbt-labs/dbt-core/issues/6781
{% endsnapshot %}
"""


@pytest.fixture(scope="class")
def snapshots():
    return {"snapshot.sql": snapshots_with_comment_at_end__snapshot_sql}


@pytest.fixture(scope="class")
def models():
    return {
        "schema.yml": models__schema_yml,
        "ref_snapshot.sql": models__ref_snapshot_sql,
    }


@pytest.fixture(scope="class")
def macros():
    return {"test_no_overlaps.sql": macros__test_no_overlaps_sql}


def test_comment_ending(project):
    results = run_dbt(["snapshot"])
    assert len(results) == 1
