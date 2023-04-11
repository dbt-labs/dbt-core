from typing import List

import pytest

from dbt.tests.util import run_dbt, get_manifest


model_base_table_sql = """
{{ config(materialized='table') }}
select 1 as base_column
"""


model_mat_view_sql = """
{{ config(materialized='materialized_view') }}
select * from {{ ref('base_table') }}
"""


@pytest.fixture(scope="class")
def models():
    return {
        "base_table.sql": model_base_table_sql,
        "mat_view.sql": model_mat_view_sql,
    }


def get_records(project, relation_name: str) -> List[tuple]:
    sql = f"select * from {project.database}.{project.test_schema}.{relation_name};"
    return project.run_sql(sql, fetch="all")


def insert_record(project, relation_name: str, columns: List[str], record: tuple):
    sql = f"""
    insert into {project.database}.{project.test_schema}.{relation_name} ({', '.join(columns)})
    values ({','.join(str(value) for value in record)})
    ;"""
    project.run_sql(sql)


def test_materialized_view_gets_created(project):
    results = run_dbt()
    assert len(results) == 2  # 1 table and 1 materialized view
    assert get_records(project, "mat_view") == [(1,)]

    manifest = get_manifest(project.project_root)
    model = manifest.nodes["model.test.mat_view"]
    assert model.config.materialized == "materialized_view"


def test_materialized_view_gets_created_after_full_refresh(project):
    run_dbt()

    insert_record(project, "base_table", ["base_column"], (2,))
    run_dbt(["run", "--models", "mat_view", "--full-refresh"])
    assert get_records(project, "mat_view") == [(1,), (2,)]


@pytest.mark.skip(
    "This will fail because we're using a view, which doesn't have a concept of refresh"
)
def test_updated_base_table_data_only_shows_in_materialized_view_after_rerun(project):
    run_dbt()

    insert_record(project, "base_table", ["base_column"], (2,))
    assert get_records(project, "mat_view") == [(1,)]

    run_dbt(["run", "--models", "mat_view"])
    assert get_records(project, "mat_view") == [(1,), (2,)]


def test_materialized_view_data_gets_refreshed_when_relation_exists(project):
    run_dbt()
    insert_record(project, "base_table", ["base_column"], (2,))
    run_dbt(["run", "--models", "mat_view"])

    assert get_records(project, "mat_view") == [(1,), (2,)]
