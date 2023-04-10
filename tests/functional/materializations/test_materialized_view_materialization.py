import pytest

from dbt.tests.util import run_dbt


models__model_sql = """
{{ config(materialized='materialized_view') }}
select 1 as id

"""


@pytest.fixture(scope="class")
def models():
    return {"model.sql": models__model_sql}


def test_basic(project):
    run_dbt(["run"], expect_pass=False)
