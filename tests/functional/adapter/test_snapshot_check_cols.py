import pytest
from dbt.tests.util import run_dbt
from tests.functional.adapter.files import (
    seeds_base_csv,
    seeds_added_csv,
    cc_all_snapshot_sql,
    cc_date_snapshot_sql,
    cc_name_snapshot_sql,
)
from dbt.tests.adapter import update_rows, relation_from_name


@pytest.fixture(scope="class")
def project_config_update():
    return {"name": "snapshot_strategy_check_cols"}


@pytest.fixture(scope="class")
def seeds():
    return {
        "base.csv": seeds_base_csv,
        "added.csv": seeds_added_csv,
    }


@pytest.fixture(scope="class")
def snapshots():
    return {
        "cc_all_snapshot.sql": cc_all_snapshot_sql,
        "cc_date_snapshot.sql": cc_date_snapshot_sql,
        "cc_name_snapshot.sql": cc_name_snapshot_sql,
    }


def check_relation_rows(project, snapshot_name, count):
    relation = relation_from_name(project.adapter, snapshot_name)
    result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
    assert result[0] == count


def test_snapshot_check_cols(project):
    # seed command
    results = run_dbt(["seed"])
    assert len(results) == 2

    # snapshot command
    results = run_dbt(["snapshot"])
    for result in results:
        assert result.status == "success"

    # check rowcounts for all snapshots
    check_relation_rows(project, "cc_all_snapshot", 10)
    check_relation_rows(project, "cc_name_snapshot", 10)
    check_relation_rows(project, "cc_date_snapshot", 10)

    relation = relation_from_name(project.adapter, "cc_all_snapshot")
    result = project.run_sql(f"select * from {relation}", fetch="all")

    # point at the "added" seed so the snapshot sees 10 new rows
    results = run_dbt(["--no-partial-parse", "snapshot", "--vars", "seed_name: added"])
    for result in results:
        assert result.status == "success"

    # check rowcounts for all snapshots
    check_relation_rows(project, "cc_all_snapshot", 20)
    check_relation_rows(project, "cc_name_snapshot", 20)
    check_relation_rows(project, "cc_date_snapshot", 20)

    # update some timestamps in the "added" seed so the snapshot sees 10 more new rows
    update_rows_config = {
        "name": "added",
        "dst_col": "some_date",
        "clause": {"src_col": "some_date", "type": "add_timestamp"},
        "where": "id > 10 and id < 21",
    }
    update_rows(project.adapter, update_rows_config)

    # re-run snapshots, using "added'
    results = run_dbt(["snapshot", "--vars", "seed_name: added"])
    for result in results:
        assert result.status == "success"

    # check rowcounts for all snapshots
    check_relation_rows(project, "cc_all_snapshot", 30)
    check_relation_rows(project, "cc_date_snapshot", 30)
    # unchanged: only the timestamp changed
    check_relation_rows(project, "cc_name_snapshot", 20)

    # Update the name column
    update_rows_config = {
        "name": "added",
        "dst_col": "name",
        "clause": {
            "src_col": "name",
            "type": "add_string",
            "value": "_updated",
        },
        "where": "id < 11",
    }
    update_rows(project.adapter, update_rows_config)

    # re-run snapshots, using "added'
    results = run_dbt(["snapshot", "--vars", "seed_name: added"])
    for result in results:
        assert result.status == "success"

    # check rowcounts for all snapshots
    check_relation_rows(project, "cc_all_snapshot", 40)
    check_relation_rows(project, "cc_name_snapshot", 30)
    # does not see name updates
    check_relation_rows(project, "cc_date_snapshot", 30)
