import pytest

from dbt.tests.util import relation_from_name, run_dbt

model_input_sql = """
select 1 as id
"""

ephemeral_model_input_sql = """
{{ config(materialized='ephemeral') }}
select 2 as id
"""

raw_source_csv = """id
3
"""

raw_seed_csv = """a,b,c,d,e,f
3.2,3,US,2025-01-01,none,false
4.5,4,UK,2025-01-02,2,true
"""

model_sql = """
select *
from {{ ref('model_input') }}
union all
select *
from {{ ref('ephemeral_model_input') }}
union all
select *
from {{ source('seed_sources', 'raw_source') }}
"""

model_no_ephemeral_ref_sql = """
select *
from {{ ref('model_input') }}
union all
select *
from {{ source('seed_sources', 'raw_source') }}
"""


schema_sources_yml = """
sources:
  - name: seed_sources
    schema: "{{ target.schema }}"
    tables:
      - name: raw_source
"""

unit_tests_yml = """
unit_tests:
  - name: test_my_model
    model: model_no_ephemeral_ref
    given:
      - input: ref('model_input')
        format: csv
        rows: |
          id
          1
      - input: source('seed_sources', 'raw_source')
        format: csv
        rows: |
          id
          2
    expect:
      format: csv
      rows: |
        id
        1
        2
"""

unit_tests_seed_yml = """
unit_tests:
  - name: test_my_seed
    model: model
    given:
      - input: ref('raw_seed')
    expect:
      format: csv
      rows: |
        a,b,c,d,e,f
        3.2,3,US,2025-01-01,none,false
        4.5,4,UK,2025-01-02,2,true
"""


class BaseTestEmptyFlag:
    def assert_row_count(self, project, relation_name: str, expected_row_count: int):
        relation = relation_from_name(project.adapter, relation_name)
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == expected_row_count


class TestEmptyFlag(BaseTestEmptyFlag):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_source.csv": raw_source_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_input.sql": model_input_sql,
            "ephemeral_model_input.sql": ephemeral_model_input_sql,
            "model.sql": model_sql,
            "model_no_ephemeral_ref.sql": model_no_ephemeral_ref_sql,
            "sources.yml": schema_sources_yml,
            "unit_tests.yml": unit_tests_yml,
        }

    def test_run_with_empty(self, project):
        # Create source from seed for run and build command testing
        run_dbt(["seed"])
        self.assert_row_count(project, "raw_source", 1)

        # run without empty - 3 expected rows in output - 1 from each input
        run_dbt(["run"])
        self.assert_row_count(project, "model", 3)

        # run with empty - 0 expected rows in output
        run_dbt(["run", "--empty"])
        self.assert_row_count(project, "model", 0)

        # build without empty - 3 expected rows in output - 1 from each input
        run_dbt(["build"])
        self.assert_row_count(project, "model", 3)

        # build with empty - 0 expected rows in output
        run_dbt(["build", "--empty"])
        self.assert_row_count(project, "model", 0)

        # ensure dbt compile supports --empty flag
        run_dbt(["compile", "--empty"])


class TestEmptyFlagSeed(BaseTestEmptyFlag):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_seed.csv": raw_seed_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": "select * from {{ ref('raw_seed') }}",
            "unit_tests.yml": unit_tests_seed_yml,
        }

    def test_run_with_empty(self, project):
        run_dbt(["seed", "--empty"])
        self.assert_row_count(project, "raw_seed", 0)

        run_dbt(["build"])
