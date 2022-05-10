import pytest
from dbt.tests.util import run_dbt, check_relations_equal

snapshot_sql = """
{% snapshot snapshot_check_cols_new_column %}
    {{
        config(
            target_database=database,
            target_schema=schema,
            strategy='check',
            unique_key='id',
            check_cols=var("check_cols", ['name']),
            updated_at="'" ~ var("updated_at") ~  "'::timestamp",
        )
    }}

    {% if var('version') == 1 %}

        select 1 as id, 'foo' as name

    {% else %}

        select 1 as id, 'foo' as name, 'bar' as other

    {% endif %}

{% endsnapshot %}
"""

expected_csv = """
id,name,other,dbt_scd_id,dbt_updated_at,dbt_valid_from,dbt_valid_to
1,foo,NULL,0d73ad1b216ad884c9f7395d799c912c,2016-07-01 00:00:00.000,2016-07-01 00:00:00.000,2016-07-02 00:00:00.000
1,foo,bar,7df3783934a6a707d51254859260b9ff,2016-07-02 00:00:00.000,2016-07-02 00:00:00.000,
""".lstrip()


@pytest.fixture(scope="class")
def snapshots():
    return {"snapshot_check_cols_new_column.sql": snapshot_sql}


@pytest.fixture(scope="class")
def seeds():
    return {"snapshot_check_cols_new_column_expected.csv": expected_csv}


@pytest.fixture(scope="class")
def project_config_update():
    return {
        "seeds": {
            "quote_columns": False,
            "test": {
                "snapshot_check_cols_new_column_expected": {
                    "+column_types": {
                        "dbt_updated_at": "timestamp without time zone",
                        "dbt_valid_from": "timestamp without time zone",
                        "dbt_valid_to": "timestamp without time zone",
                    },
                },
            },
        },
    }


def run_check_cols_snapshot_with_schema_change(project, check_cols_override=None):
    """
    Test that snapshots using the "check" strategy and explicit check_cols support adding columns.

    Approach:
    1. Take a snapshot that checks a single non-id column
    2. Add a new column to the data
    3. Take a snapshot that checks the new non-id column too

    As long as no error is thrown, then the snapshot was successful
    """

    check_cols = check_cols_override or ["name", "other"]

    # 1. Create a table that represents the expected data after a series of snapshots
    vars_dict = {"version": 1, "updated_at": "2016-07-01"}
    results = run_dbt(["seed", "--show", "--vars", str(vars_dict)])
    assert len(results) == 1

    # Snapshot 1
    # Use only 'name' for check_cols
    vars_dict = {"version": 1, "check_cols": [check_cols[0]], "updated_at": "2016-07-01"}
    results = run_dbt(["snapshot", "--vars", str(vars_dict)])
    assert len(results) == 1

    # Snapshot 2
    # Use both 'name' and 'other' for check_cols
    vars_dict = {"version": 2, "check_cols": check_cols, "updated_at": "2016-07-02"}
    results = run_dbt(["snapshot", "--vars", str(vars_dict)])
    assert len(results) == 1

    check_relations_equal(
        project.adapter,
        ["snapshot_check_cols_new_column", "snapshot_check_cols_new_column_expected"],
        compare_snapshot_cols=True,
    )

    # Snapshot 3
    # Run it again. Nothing has changed — ensure we don't detect changes
    vars_dict = {"version": 2, "check_cols": check_cols, "updated_at": "2016-07-02"}
    results = run_dbt(["snapshot", "--vars", str(vars_dict)])
    assert len(results) == 1

    check_relations_equal(
        project.adapter,
        ["snapshot_check_cols_new_column", "snapshot_check_cols_new_column_expected"],
        compare_snapshot_cols=True,
    )


def test_check_cols_snapshot_with_schema_change(project):
    run_check_cols_snapshot_with_schema_change(project)


def test_check_cols_snapshot_with_schema_change_and_mismatched_casing(project):
    """
    Test that this still works if the database-stored version of 'name' + 'other'
    differs from the user-configured 'NAME' and 'OTHER'
    """
    run_check_cols_snapshot_with_schema_change(
        project=project, check_cols_override=["NAME", "OTHER"]
    )
