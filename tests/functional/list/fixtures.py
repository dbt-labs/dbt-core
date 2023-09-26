import pytest
from dbt.tests.fixtures.project import write_project_files


snapshots__snapshot_sql = """
{% snapshot my_snapshot %}
    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select * from {{database}}.{{schema}}.seed
{% endsnapshot %}

"""

tests__t_sql = """
select 1 as id limit 0

"""

models__schema_yml = """
version: 2
models:
  - name: outer
    description: The outer table
    columns:
      - name: id
        description: The id value
        tests:
          - unique
          - not_null

sources:
  - name: my_source
    tables:
      - name: my_table

"""

models__ephemeral_sql = """

{{ config(materialized='ephemeral') }}

select
  1 as id,
  {{ dbt.date_trunc('day', dbt.current_timestamp()) }} as created_at

"""

models__metric_flow = """

select
  {{ dbt.date_trunc('day', dbt.current_timestamp()) }} as date_day

"""

models__incremental_sql = """
{{
  config(
    materialized = "incremental",
    incremental_strategy = "delete+insert",
  )
}}

select * from {{ ref('seed') }}

{% if is_incremental() %}
    where a > (select max(a) from {{this}})
{% endif %}

"""

models__docs_md = """
{% docs my_docs %}
  some docs
{% enddocs %}

"""

models__outer_sql = """
select * from {{ ref('ephemeral') }}

"""

models__sub__inner_sql = """
select * from {{ ref('outer') }}

"""

macros__macro_stuff_sql = """
{% macro cool_macro() %}
  wow!
{% endmacro %}

{% macro other_cool_macro(a, b) %}
  cool!
{% endmacro %}

"""

seeds__seed_csv = """a,b
1,2
"""

analyses__a_sql = """
select 4 as id

"""

semantic_models__sm_yml = """
semantic_models:
  - name: my_sm
    model: ref('outer')
    defaults:
      agg_time_dimension: created_at
    entities:
      - name: my_entity
        type: primary
        expr: id
    dimensions:
      - name: created_at
        type: time
        type_params:
          time_granularity: day
    measures:
      - name: total_outer_count
        agg: count
        expr: 1

"""

metrics__m_yml = """
metrics:
  - name: total_outer
    type: simple
    description: The total count of outer
    label: Total Outer
    type_params:
      measure: total_outer_count
"""


@pytest.fixture(scope="class")
def snapshots():
    return {"snapshot.sql": snapshots__snapshot_sql}


@pytest.fixture(scope="class")
def tests():
    return {"t.sql": tests__t_sql}


@pytest.fixture(scope="class")
def models():
    return {
        "schema.yml": models__schema_yml,
        "ephemeral.sql": models__ephemeral_sql,
        "incremental.sql": models__incremental_sql,
        "docs.md": models__docs_md,
        "outer.sql": models__outer_sql,
        "metricflow_time_spine.sql": models__metric_flow,
        "sm.yml": semantic_models__sm_yml,
        "m.yml": metrics__m_yml,
        "sub": {"inner.sql": models__sub__inner_sql},
    }


@pytest.fixture(scope="class")
def macros():
    return {"macro_stuff.sql": macros__macro_stuff_sql}


@pytest.fixture(scope="class")
def seeds():
    return {"seed.csv": seeds__seed_csv}


@pytest.fixture(scope="class")
def analyses():
    return {"a.sql": analyses__a_sql}


@pytest.fixture(scope="class")
def semantic_models():
    return {"sm.yml": semantic_models__sm_yml}


@pytest.fixture(scope="class")
def metrics():
    return {"m.yml": metrics__m_yml}


@pytest.fixture(scope="class")
def project_files(
    project_root,
    snapshots,
    tests,
    models,
    macros,
    seeds,
    analyses,
):
    write_project_files(project_root, "snapshots", snapshots)
    write_project_files(project_root, "tests", tests)
    write_project_files(project_root, "models", models)
    write_project_files(project_root, "macros", macros)
    write_project_files(project_root, "seeds", seeds)
    write_project_files(project_root, "analyses", analyses)
