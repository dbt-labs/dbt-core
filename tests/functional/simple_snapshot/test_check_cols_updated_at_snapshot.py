import pytest
from dbt.tests.util import run_dbt, check_relations_equal

snapshot_sql = """
{% snapshot snapshot_check_cols_updated_at_actual %}
    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols='all',
            updated_at="'" ~ var("updated_at") ~  "'::timestamp",
        )
    }}

    {% if var('version') == 1 %}

        select 'a' as id, 10 as counter, '2016-01-01T00:00:00Z'::timestamp as timestamp_col union all
        select 'b' as id, 20 as counter, '2016-01-01T00:00:00Z'::timestamp as timestamp_col

    {% elif var('version') == 2 %}

        select 'a' as id, 30 as counter, '2016-01-02T00:00:00Z'::timestamp as timestamp_col union all
        select 'b' as id, 20 as counter, '2016-01-01T00:00:00Z'::timestamp as timestamp_col union all
        select 'c' as id, 40 as counter, '2016-01-02T00:00:00Z'::timestamp as timestamp_col

    {% else %}

        select 'a' as id, 30 as counter, '2016-01-02T00:00:00Z'::timestamp as timestamp_col union all
        select 'c' as id, 40 as counter, '2016-01-02T00:00:00Z'::timestamp as timestamp_col

    {% endif %}

{% endsnapshot %}
"""

expected_csv = """
id,counter,timestamp_col,dbt_scd_id,dbt_updated_at,dbt_valid_from,dbt_valid_to
a,10,2016-01-01 00:00:00.000,927354aa091feffd9437ead0bdae7ae1,2016-07-01 00:00:00.000,2016-07-01 00:00:00.000,2016-07-02 00:00:00.000
b,20,2016-01-01 00:00:00.000,40ace4cbf8629f1720ec8a529ed76f8c,2016-07-01 00:00:00.000,2016-07-01 00:00:00.000,
a,30,2016-01-02 00:00:00.000,e9133f2b302c50e36f43e770944cec9b,2016-07-02 00:00:00.000,2016-07-02 00:00:00.000,
c,40,2016-01-02 00:00:00.000,09d33d35101e788c152f65d0530b6837,2016-07-02 00:00:00.000,2016-07-02 00:00:00.000,
""".lstrip()


@pytest.fixture(scope="class")
def snapshots():
    return {"snapshot_check_cols_updated_at_actual.sql": snapshot_sql}


@pytest.fixture(scope="class")
def seeds():
    return {"snapshot_check_cols_updated_at_expected.csv": expected_csv}


@pytest.fixture(scope="class")
def project_config_update():
    return {
        "seeds": {
            "quote_columns": False,
            "test": {
                "snapshot_check_cols_updated_at_expected": {
                    "+column_types": {
                        "timestamp_col": "timestamp without time zone",
                        "dbt_updated_at": "timestamp without time zone",
                        "dbt_valid_from": "timestamp without time zone",
                        "dbt_valid_to": "timestamp without time zone",
                    },
                },
            },
        },
    }


def test_simple_snapshot(project):
    """
    Test that the `dbt_updated_at` column reflects the `updated_at` timestamp expression in the config.

    Approach:
    1. Create a table that represents the expected data after a series of snapshots
        - Use dbt seed to create the expected relation (`snapshot_check_cols_updated_at_expected`)
    2. Execute a series of snapshots to create the data
        - Use a series of (3) dbt snapshot commands to create the actual relation (`snapshot_check_cols_updated_at_actual`)
        - The logic can switch between 3 different versions of the data (depending on the `version` number)
        - The `updated_at` value is passed in via `--vars` and cast to a timestamp in the snapshot config
    3. Compare the two relations for equality
    """

    # 1. Create a table that represents the expected data after a series of snapshots
    results = run_dbt(["seed", "--show", "--vars", "{version: 1, updated_at: 2016-07-01}"])
    assert len(results) == 1

    # 2. Execute a series of snapshots to create the data

    # Snapshot day 1
    results = run_dbt(["snapshot", "--vars", "{version: 1, updated_at: 2016-07-01}"])
    assert len(results) == 1

    # Snapshot day 2
    results = run_dbt(["snapshot", "--vars", "{version: 2, updated_at: 2016-07-02}"])
    assert len(results) == 1

    # Snapshot day 3
    results = run_dbt(["snapshot", "--vars", "{version: 3, updated_at: 2016-07-03}"])
    assert len(results) == 1

    # 3. Compare the two relations for equality
    check_relations_equal(
        project.adapter,
        ["snapshot_check_cols_updated_at_actual", "snapshot_check_cols_updated_at_expected"],
        compare_snapshot_cols=True,
    )
