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
def model(dbt, _):
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


m_1 = """
{{config(materialized='table')}}
select 1 as id union all
select 2 as id union all
select 3 as id union all
select 4 as id union all
select 5 as id
"""

incremental_python = """
def model(dbt, session):
    dbt.config(materialized="incremental", unique_key='id')
    df = dbt.ref("m_1")
    if dbt.is_incremental:
        # incremental runs should only apply to part of the data
        df = df.filter(df.id > 5)
    return df
"""


class BasePythonIncrementalTests:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "merge"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"m_1.sql": m_1, "incremental.py": incremental_python}

    def test_incremental(self, project):
        # create m_1 and run incremental model the first time
        run_dbt(["run"])
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        assert (
            project.run_sql(
                f"select count(*) from {test_schema_relation}.incremental",
                fetch="one",
            )[0]
            == 5
        )
        # running incremental model again will not cause any changes in the result model
        run_dbt(["run", "-s", "incremental"])
        assert (
            project.run_sql(
                f"select count(*) from {test_schema_relation}.incremental",
                fetch="one",
            )[0]
            == 5
        )
        # add 3 records with one supposed to be filtered out
        project.run_sql(f"insert into {test_schema_relation}.m_1(id) values (0), (6), (7)")
        # validate that incremental model would correctly add 2 valid records to result model
        run_dbt(["run", "-s", "incremental"])
        assert (
            project.run_sql(
                f"select count(*) from {test_schema_relation}.incremental",
                fetch="one",
            )[0]
            == 7
        )
