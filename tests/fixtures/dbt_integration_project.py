import pytest

dbt_integration_project__my_macros_sql = """


{% macro do_something(foo, bar) %}

    select
        '{{ foo }}'::text as foo,
        '{{ bar }}'::text as bar

{% endmacro %}

"""


dbt_integration_project__incremental_sql = """

-- TODO : add dist/sort keys
{{
    config(
        materialized = 'incremental',
        unique_key   = 'id',
    )
}}


select * from {{ this.schema }}.seed

{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
"""


dbt_integration_project__schema_yml = """
version: 2
models:
- name: table_model
  columns:
  - name: id
    tests:
    - unique
"""


dbt_integration_project__table_model_sql = """

-- TODO : add dist/sort keys
{{
    config(
        materialized = 'table',
    )
}}

select * from {{ this.schema }}.seed
"""

dbt_integration_project__view_model_sql = """
{{
    config(
        materialized = 'view',
    )
}}

select * from {{ this.schema }}.seed
"""


dbt_integration_project__dbt_project_yml = """
name: dbt_integration_project
version: '1.0'
config-version: 2

model-paths: ["models"]    # paths to models
analysis-paths: ["analyses"] # path with analysis files which are compiled, but not run
target-path: "target"      # path for compiled code
clean-targets: ["target"]  # directories removed by the clean task
test-paths: ["tests"]       # where to store test results
seed-paths: ["seeds"]       # load CSVs from this directory with `dbt seed`
macro-paths: ["macros"]    # where to find macros

profile: user

models:
    dbt_integration_project:
"""


@pytest.fixture(scope="class")
def dbt_integration_project():
    return {
        "dbt_project.yml": dbt_integration_project__dbt_project_yml,
        "macros": {"my_macros.sql": dbt_integration_project__my_macros_sql},
        "models": {
            "incremental.sql": dbt_integration_project__incremental_sql,
            "schema.yml": dbt_integration_project__schema_yml,
            "table_model.sql": dbt_integration_project__table_model_sql,
            "view_model.sql": dbt_integration_project__view_model_sql,
        },
    }
