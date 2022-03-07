import pytest
from dbt.tests.util import run_dbt, copy_file, read_file
from dbt.tests.tables import TableComparison

# advanced_incremental.sql
advanced_incremental_sql = """
{{
  config(
    materialized = "incremental",
    unique_key = "id",
    persist_docs = {"relation": true}
  )
}}

select *
from {{ ref('seed') }}

{% if is_incremental() %}

    where id > (select max(id) from {{this}})

{% endif %}
"""

# compound_sort.sql
compound_sort_sql = """
{{
  config(
    materialized = "table",
    sort = 'first_name',
    sort_type = 'compound'
  )
}}

select * from {{ ref('seed') }}
"""

# disabled.sql
disabled_sql = """
{{
  config(
    materialized = "view",
    enabled = False
  )
}}

select * from {{ ref('seed') }}
"""

# empty.sql
empty_sql = """
"""


# get_and_ref.sql
get_and_ref_sql = """
{%- do adapter.get_relation(database=target.database, schema=target.schema, identifier='materialized') -%}

select * from {{ ref('materialized') }}
"""

# incremental.sql
incremental_sql = """
{{
  config(
    materialized = "incremental"
  )
}}

select * from {{ ref('seed') }}

{% if is_incremental() %}
    where id > (select max(id) from {{this}})
{% endif %}
"""

# interleaved_sort.sql
interleaved_sort_sql = """
{{
  config(
    materialized = "table",
    sort = ['first_name', 'last_name'],
    sort_type = 'interleaved'
  )
}}

select * from {{ ref('seed') }}
"""

# materialized.sql
materialized_sql = """
{{
  config(
    materialized = "table"
  )
}}
-- ensure that dbt_utils' relation check will work
{% set relation = ref('seed') %}
{%- if not (relation is mapping and relation.get('metadata', {}).get('type', '').endswith('Relation')) -%}
    {%- do exceptions.raise_compiler_error("Macro " ~ macro ~ " expected a Relation but received the value: " ~ relation) -%}
{%- endif -%}
-- this is a unicode character: å
select * from {{ relation }}
"""

# schema.yml
schema_yml = """
version: 2
models:
- name: disabled
  columns:
  - name: id
    tests:
    - unique
"""

# view_model.sql
view_model_sql = """
{{
  config(
    materialized = "view"
  )
}}

select * from {{ ref('seed') }}
"""


@pytest.fixture(scope="class")
def models():
    return {
        "advanced_incremental.sql": advanced_incremental_sql,
        "compound_sort.sql": compound_sort_sql,
        "disabled.sql": disabled_sql,
        "empty.sql": empty_sql,
        "get_and_ref.sql": get_and_ref_sql,
        "incremental.sql": incremental_sql,
        "interleaved_sort.sql": interleaved_sort_sql,
        "materialized.sql": materialized_sql,
        "schema.yml": schema_yml,
        "view_model.sql": view_model_sql,
    }


@pytest.fixture(scope="class")
def seeds(test_data_dir):
    # Read seed file and return
    seed_csv = read_file(test_data_dir, "seed-initial.csv")
    return {"seed.csv": seed_csv}


@pytest.fixture(scope="class")
def project_config_update():
    return {"seeds": {"quote_columns": False}}


def test_simple_copy(project, test_data_dir):

    # Load the seed file and check that it worked
    results = run_dbt(["seed"])
    assert len(results) == 1

    # Run the project and ensure that all the models loaded
    results = run_dbt()
    assert len(results) == 7
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_many_tables_equal(
        ["seed", "view_model", "incremental", "materialized", "get_and_ref"]
    )

    # Change the seed.csv file and see if everything is the same, i.e. everything has been updated
    copy_file(test_data_dir, "seed-update.csv", project.project_root, ["seeds", "seed.csv"])
    results = run_dbt(["seed"])
    assert len(results) == 1
    results = run_dbt()
    assert len(results) == 7
    table_comp.assert_many_tables_equal(
        ["seed", "view_model", "incremental", "materialized", "get_and_ref"]
    )


def test_simple_copy_with_materialized_views(project):
    project.run_sql(f"create table {project.test_schema}.unrelated_table (id int)")
    sql = f"""
        create materialized view {project.test_schema}.unrelated_materialized_view as (
            select * from {project.test_schema}.unrelated_table
        )
    """
    project.run_sql(sql)
    sql = f"""
        create view {project.test_schema}.unrelated_view as (
            select * from {project.test_schema}.unrelated_materialized_view
        )
    """
    project.run_sql(sql)
    results = run_dbt(["seed"])
    assert len(results) == 1
    results = run_dbt()
    assert len(results) == 7


def test_dbt_doesnt_run_empty_models(project):
    results = run_dbt(["seed"])
    assert len(results) == 1
    results = run_dbt()
    assert len(results) == 7

    tables = project.get_tables_in_schema()

    assert "empty" not in tables.keys()
    assert "disabled" not in tables.keys()
