import pytest
from dbt.tests.util import run_dbt
from tests.functional.simple_snapshot.fixtures import (
    models__schema_yml,
    models__ref_snapshot_sql,
    macros__test_no_overlaps_sql,
)

seed_longtext_sql = """
create table {database}.{schema}.super_long (
    id INTEGER,
    longstring TEXT,
    updated_at TIMESTAMP WITHOUT TIME ZONE
);

insert into {database}.{schema}.super_long (id, longstring, updated_at) VALUES
(1, 'short', current_timestamp),
(2, repeat('a', 500), current_timestamp);
"""

snapshots_longtext__snapshot_sql = """
{% snapshot snapshot_actual %}
    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select * from {{target.database}}.{{schema}}.super_long
{% endsnapshot %}
"""


@pytest.fixture(scope="class")
def snapshots():
    return {"snapshot.sql": snapshots_longtext__snapshot_sql}


@pytest.fixture(scope="class")
def models():
    return {
        "schema.yml": models__schema_yml,
        "ref_snapshot.sql": models__ref_snapshot_sql,
    }


@pytest.fixture(scope="class")
def macros():
    return {"test_no_overlaps.sql": macros__test_no_overlaps_sql}


def test_long_text(project):
    project.run_sql(seed_longtext_sql)

    results = run_dbt(["snapshot"])
    assert len(results) == 1

    with project.adapter.connection_named("test"):
        status, results = project.adapter.execute(
            "select * from {}.{}.snapshot_actual".format(project.database, project.test_schema),
            fetch=True,
        )
    assert len(results) == 2
    got_names = set(r.get("longstring") for r in results)
    assert got_names == {"a" * 500, "short"}
