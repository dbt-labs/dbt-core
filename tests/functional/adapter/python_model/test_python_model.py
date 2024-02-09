import pytest
import os
import yaml
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
    df2 = dbt.ref("my_versioned_sql_model", v=1)
    df3 = dbt.ref("my_versioned_sql_model", version=1)
    df4 = dbt.ref("test", "my_versioned_sql_model", v=1)
    df5 = dbt.ref("test", "my_versioned_sql_model", version=1)
    df6 = dbt.source("test_source", "test_table")
    df = df.limit(2)
    return df
"""

second_sql = """
select * from {{ref('my_python_model')}}
"""
schema_yml = """version: 2
models:
  - name: my_versioned_sql_model
    versions:
      - v: 1

sources:
  - name: test_source
    loader: custom
    schema: "{{ var(env_var('DBT_TEST_SCHEMA_NAME_VARIABLE')) }}"
    quoting:
      identifier: True
    tags:
      - my_test_source_tag
    tables:
      - name: test_table
        identifier: source
"""

seeds__source_csv = """favorite_color,id,first_name,email,ip_address,updated_at
blue,1,Larry,lking0@miitbeian.gov.cn,'69.135.206.194',2008-09-12 19:08:31
blue,2,Larry,lperkins1@toplist.cz,'64.210.133.162',1978-05-09 04:15:14
"""


class BasePythonModelTests:
    @pytest.fixture(scope="class", autouse=True)
    def setEnvVars(self):
        os.environ["DBT_TEST_SCHEMA_NAME_VARIABLE"] = "test_run_schema"

        yield

        del os.environ["DBT_TEST_SCHEMA_NAME_VARIABLE"]

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"source.csv": seeds__source_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "my_sql_model.sql": basic_sql,
            "my_versioned_sql_model_v1.sql": basic_sql,
            "my_python_model.py": basic_python,
            "second_sql_model.sql": second_sql,
        }

    def test_singular_tests(self, project):
        # test command
        vars_dict = {
            "test_run_schema": project.test_schema,
        }

        run_dbt(["seed", "--vars", yaml.safe_dump(vars_dict)])
        results = run_dbt(["run", "--vars", yaml.safe_dump(vars_dict)])
        assert len(results) == 4


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
