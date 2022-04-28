import pytest

models__schema_yml = """
version: 2
sources:
  - name: raw
    database: "{{ target.database }}"
    schema: "{{ target.schema }}"
    tables:
      - name: 'seed'
        identifier: "{{ var('seed_name', 'invalid') }}"
        columns:
          - name: id
            tests:
              - unique:
                  enabled: "{{ var('enabled_direct', None) | as_native }}"
              - accepted_values:
                  enabled: "{{ var('enabled_direct', None) | as_native }}"
                  severity: "{{ var('severity_direct', None) | as_native }}"
                  values: [1,2]

models:
  - name: model
    columns:
      - name: id
        tests:
          - unique
          - accepted_values:
              values: [1,2,3,4]

"""

models__untagged_sql = """
{{
    config(materialized='table')
}}

select id, value from {{ source('raw', 'seed') }}

"""

models__tagged__model_sql = """
{{
    config(
        materialized='view',
        tags=['tag_two'],
    )
}}

{{
    config(
        materialized='table',
        tags=['tag_three'],
    )
}}

select 4 as id, 2 as value

"""

seeds__seed_csv = """id,value
4,2
"""

tests__failing_sql = """

select 1 as fun

"""

tests__sleeper_agent_sql = """
{{ config(
    enabled = var('enabled_direct', False),
    severity = var('severity_direct', 'WARN')
) }}

select 1 as fun

"""


class BaseConfigProject:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
            "untagged.sql": models__untagged_sql,
            "tagged": {"model.sql": models__tagged__model_sql},
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds__seed_csv}

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "failing.sql": tests__failing_sql,
            "sleeper_agent.sql": tests__sleeper_agent_sql,
        }
