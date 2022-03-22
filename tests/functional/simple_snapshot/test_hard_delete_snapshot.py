import os
from datetime import datetime
import pytz
import pytest
from dbt.tests.util import run_dbt
from dbt.tests.tables import TableComparison
from tests.functional.simple_snapshot.fixtures import (
    models__schema_yml,
    models__ref_snapshot_sql,
    macros__test_no_overlaps_sql,
    snapshots_pg__snapshot_sql,
)


# These tests uses the same seed data, containing 20 records of which we hard delete the last 10.
# These deleted records set the dbt_valid_to to time the snapshot was ran.


def datetime_snapshot():
    NUM_SNAPSHOT_MODELS = 1
    begin_snapshot_datetime = datetime.now(pytz.UTC)
    results = run_dbt(["snapshot", "--vars", "{invalidate_hard_deletes: true}"])
    assert len(results) == NUM_SNAPSHOT_MODELS

    return begin_snapshot_datetime


@pytest.fixture(scope="class")
def snapshots():
    return {"snapshot.sql": snapshots_pg__snapshot_sql}


@pytest.fixture(scope="class")
def models():
    return {
        "schema.yml": models__schema_yml,
        "ref_snapshot.sql": models__ref_snapshot_sql,
    }


@pytest.fixture(scope="class")
def macros():
    return {"test_no_overlaps.sql": macros__test_no_overlaps_sql}


def test_snapshot_hard_delete(project):
    path = os.path.join(project.test_data_dir, "seed_pg.sql")
    project.run_sql_file(path)

    datetime_snapshot()

    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )

    table_comp.assert_tables_equal("snapshot_expected", "snapshot_actual")

    invalidated_snapshot_datetime = None
    revived_snapshot_datetime = None

    # hard delete last 10 records
    project.run_sql(
        "delete from {}.{}.seed where id >= 10;".format(project.database, project.test_schema)
    )

    # snapshot and assert invalidated
    invalidated_snapshot_datetime = datetime_snapshot()

    snapshotted = project.run_sql(
        """
        select
            id,
            dbt_valid_to
        from {}.{}.snapshot_actual
        order by id
        """.format(
            project.database, project.test_schema
        ),
        fetch="all",
    )

    assert len(snapshotted) == 20
    for result in snapshotted[10:]:
        # result is a tuple, the dbt_valid_to column is the latest
        assert isinstance(result[-1], datetime)
        assert result[-1].replace(tzinfo=pytz.UTC) >= invalidated_snapshot_datetime

    # revive records
    revival_timestamp = datetime.now(pytz.UTC).strftime(r"%Y-%m-%d %H:%M:%S")
    project.run_sql(
        """
        insert into {}.{}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
        (10, 'Rachel', 'Lopez', 'rlopez9@themeforest.net', 'Female', '237.165.82.71', '{}'),
        (11, 'Donna', 'Welch', 'dwelcha@shutterfly.com', 'Female', '103.33.110.138', '{}')
        """.format(
            project.database, project.test_schema, revival_timestamp, revival_timestamp
        )
    )

    # snapshot and assert records were revived
    revived_snapshot_datetime = datetime_snapshot()

    # records which weren't revived (id != 10, 11)
    invalidated_records = project.run_sql(
        """
        select
            id,
            dbt_valid_to
        from {}.{}.snapshot_actual
        where dbt_valid_to is not null
        order by id
        """.format(
            project.database, project.test_schema
        ),
        fetch="all",
    )

    assert len(invalidated_records) == 11
    for result in invalidated_records:
        # result is a tuple, the dbt_valid_to column is the latest
        assert isinstance(result[1], datetime)
        assert result[1].replace(tzinfo=pytz.UTC) >= invalidated_snapshot_datetime

    # records which were revived (id = 10, 11)
    revived_records = project.run_sql(
        """
        select
            id,
            dbt_valid_from,
            dbt_valid_to
        from {}.{}.snapshot_actual
        where dbt_valid_to is null
        and id IN (10, 11)
        """.format(
            project.database, project.test_schema
        ),
        fetch="all",
    )

    assert len(revived_records) == 2
    for result in revived_records:
        # result is a tuple, the dbt_valid_from is second and dbt_valid_to is latest
        assert isinstance(result[1], datetime)
        # there are milliseconds (part of microseconds in datetime objects) in the
        # revived_snapshot_datetime and not in result datetime so set the microseconds to 0
        assert result[1].replace(tzinfo=pytz.UTC) >= revived_snapshot_datetime.replace(
            microsecond=0
        )
        assert result[2] is None
