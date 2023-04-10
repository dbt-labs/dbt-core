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
    """
    DDL is not implemented at the abstract `dbt-core` layer; this is expected to fail.

    See `tests/adapter/dbt/tests/adapter/materialized_view_tests/` for relevant tests
    """
    run_dbt(["run"], expect_pass=False)
