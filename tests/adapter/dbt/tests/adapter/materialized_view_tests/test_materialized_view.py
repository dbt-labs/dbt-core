from typing import List

import pytest

from dbt.tests.util import run_dbt

from tests.adapter.dbt.tests.adapter.materialized_view_tests import model_fixtures


RecordSet = List[tuple]


def get_records(project, relation_name) -> RecordSet:
    sql = f"select * from {project.database}.{project.test_schema}.{relation_name}"
    return [tuple(row) for row in project.run_sql(sql, fetch="all")]


@pytest.fixture(scope="class")
def models():
    return {
        "base_table.sql": model_fixtures.MODEL_BASE_TABLE,
        "mat_view.sql": model_fixtures.MODEL_MAT_VIEW,
    }


def test_create_materialized_view(project):
    """
    Run dbt once to set up the table and view
    Verify that the view sees the table
    Update the table
    Verify that the view sees the update
    """
    run_dbt()
    records = get_records(project, "mat_view")
    assert records == [(1,)]

    sql = f"insert into {project.database}.{project.test_schema}.base_table (my_col) values (2)"
    project.run_sql(sql)

    records = get_records(project, "mat_view")
    assert sorted(records) == sorted([(1,), (2,)])
