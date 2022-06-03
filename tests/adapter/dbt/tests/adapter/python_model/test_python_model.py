import pytest

from dbt.tests.util import run_dbt

basic_sql = """
select 1 as id union all
select 1 as id union all
select 1 as id union all
select 1 as id union all
select 1 as id union all
select 1 as id
"""
basic_python = """
def model(dbt):
    dbt.config(
        materialized='table',
    )
    df =  dbt.ref("my_sql_model")
    df = df.limit(2)
    return df
"""

second_sql = """
select * from {{ref('my_python_model')}}
"""


class BasePythonModelTests:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_sql_model.sql": basic_sql,
            "my_python_model.py": basic_python,
            "second_sql_model.sql": second_sql,
        }

    def test_singular_tests(self, project):
        # test command
        results = run_dbt(["run"])
        assert len(results) == 3
