import pytest
from dbt.tests.util import run_dbt
from dbt.exceptions import ParsingException
from tests.functional.simple_snapshot.fixtures import (
    models__schema_yml,
    models__ref_snapshot_sql,
    macros__test_no_overlaps_sql,
)

snapshots_invalid__snapshot_sql = """
{# make sure to never name this anything with `target_schema` in the name, or the test will be invalid! #}
{% snapshot missing_field_target_underscore_schema %}
    {# missing the mandatory target_schema parameter #}
    {{
        config(
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select * from {{target.database}}.{{schema}}.seed

{% endsnapshot %}
"""


@pytest.fixture(scope="class")
def snapshots():
    return {"snapshot.sql": snapshots_invalid__snapshot_sql}


@pytest.fixture(scope="class")
def models():
    return {
        "schema.yml": models__schema_yml,
        "ref_snapshot.sql": models__ref_snapshot_sql,
    }


@pytest.fixture(scope="class")
def macros():
    return {"test_no_overlaps.sql": macros__test_no_overlaps_sql}


def test_missing_strategy(project):
    with pytest.raises(ParsingException) as exc:
        run_dbt(["compile"], expect_pass=False)

    assert "Snapshots must be configured with a 'strategy'" in str(exc.value)
