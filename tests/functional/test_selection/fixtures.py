import pytest
from dbt.tests.fixtures.project import write_project_files


tests__cf_a_b_sql = """
select * from {{ ref('model_a') }}
cross join {{ ref('model_b') }}
where false
"""

tests__cf_a_src_sql = """
select * from {{ ref('model_a') }}
cross join {{ source('my_src', 'my_tbl') }}
where false
"""

tests__just_a_sql = """
{{ config(tags = ['data_test_tag']) }}

select * from {{ ref('model_a') }}
where false
"""

models__schema_yml = """
version: 2

sources:
  - name: my_src
    schema: "{{ target.schema }}"
    tables:
      - name: my_tbl
        identifier: model_b
        columns:
          - name: fun
            tests:
              - unique

models:
  - name: model_a
    columns:
      - name: fun
        tags: [column_level_tag]
        tests:
          - unique
          - relationships:
              to: ref('model_b')
              field: fun
              tags: [test_level_tag]
          - relationships:
              to: source('my_src', 'my_tbl')
              field: fun
"""

models__model_b_sql = """
{{ config(
    tags = ['a_or_b']
) }}

select 1 as fun
"""

models__model_a_sql = """
{{ config(
    tags = ['a_or_b']
) }}

select 1 as fun
"""


@pytest.fixture(scope="class")
def tests():
    return {
        "cf_a_b.sql": tests__cf_a_b_sql,
        "cf_a_src.sql": tests__cf_a_src_sql,
        "just_a.sql": tests__just_a_sql,
    }


@pytest.fixture(scope="class")
def models():
    return {
        "schema.yml": models__schema_yml,
        "model_b.sql": models__model_b_sql,
        "model_a.sql": models__model_a_sql,
    }


@pytest.fixture(scope="class")
def project_files(
    project_root,
    tests,
    models,
):
    write_project_files(project_root, "tests", tests)
    write_project_files(project_root, "models", models)
