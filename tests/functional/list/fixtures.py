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

select 1 as id

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
