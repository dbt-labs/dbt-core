import pytest

from dbt.tests.util import run_dbt

# Seed data for source tables
seeds__source_table_csv = """id,_loaded_at
1,2024-03-20 00:00:00
2,2024-03-20 00:00:00
3,2024-03-20 00:00:00
"""


models__no_freshness_sql = """
select 1 as id
"""

# Scenario 2: Model freshness defined with just model freshness spec
models__model_freshness_schema_yml = """
version: 2

sources:
  - name: my_source
    database: "{{ target.database }}"
    schema: "{{ target.schema }}"
    config:
      freshness:
        warn_after: {count: 24, period: hour}
        error_after: {count: 48, period: hour}
    loaded_at_field: _loaded_at
    tables:
      - name: source_table
        identifier: source_table

models:
  - name: model_a
    description: Model with no freshness defined
  - name: model_b
    description: Model with only model freshness defined
    config:
      freshness:
        build_after:
          count: 1
          period: day
          updates_on: all
  - name: model_c
    description: Model with only source freshness defined
    config:
      freshness:
        warn_after: {count: 24, period: hour}
        error_after: {count: 48, period: hour}
    loaded_at_field: _loaded_at
    tables:
      - name: source_table
        identifier: source_table
"""

models__model_freshness_sql = """
select 1 as id
"""

models__model_freshness_sql_inline = """
{{ config(
    materialized='table',
    freshness={
        'warn_after': {'count': 24, 'period': 'hour'}
    }
) }}
select 1 as id
"""

models__source_freshness_sql = """
select * from {{ source('my_source', 'source_table') }}
"""

models__both_freshness_sql = """
select * from {{ source('my_source', 'source_table') }}
"""


class TestModelFreshnessConfig:

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__model_freshness_schema_yml,
            "model_a.sql": models__no_freshness_sql,
            "model_b.sql": models__model_freshness_sql,
            "model_c.sql": models__source_freshness_sql,
            "model_d.sql": models__both_freshness_sql,
            "model_e.sql": models__model_freshness_sql_inline,
        }

    def test_model_freshness_configs(self, project):
        run_dbt(["parse"])
        compile_results = run_dbt(["compile"])
        assert len(compile_results) == 5  # All 4 models compiled successfully
