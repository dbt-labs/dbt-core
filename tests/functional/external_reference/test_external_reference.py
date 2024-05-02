import pytest

from dbt.tests.util import run_dbt

external_model_sql = """
{{
  config(
    materialized = "view"
  )
}}

select * from "{{ this.schema + 'z' }}"."external"
"""

model_sql = """
select 1 as id
"""


class TestExternalReference:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": external_model_sql}

    def test_external_reference(self, project, unique_schema):
        external_schema = unique_schema + "z"
        project.run_sql(f'create schema "{external_schema}"')
        project.run_sql(f'create table "{external_schema}"."external" (id integer)')
        project.run_sql(f'insert into "{external_schema}"."external" values (1), (2)')

        results = run_dbt(["run"])
        assert len(results) == 1

        # running it again should succeed
        results = run_dbt(["run"])
        assert len(results) == 1


# The opposite of the test above -- check that external relations that
# depend on a dbt model do not create issues with caching
class TestExternalDependency:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": model_sql}

    def test_external_reference(self, project, unique_schema):
        results = run_dbt(["run"])
        assert len(results) == 1

        external_schema = unique_schema + "z"
        project.run_sql(f'create schema "{external_schema}"')
        project.run_sql(
            f'create view "{external_schema}"."external" as (select * from {unique_schema}.model)'
        )

        # running it again should succeed
        results = run_dbt(["run"])
        assert len(results) == 1
