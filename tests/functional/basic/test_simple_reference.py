import pytest
import os
from dbt.tests.util import run_dbt, run_sql_file
from dbt.tests.tables import TableComparison, get_tables_in_schema


ephemeral_copy_sql = """
{{
  config(
    materialized = "ephemeral"
  )
}}

select * from {{ this.schema }}.seed
"""

ephemeral_summary_sql = """
{{
  config(
    materialized = "table"
  )
}}

select gender, count(*) as ct from {{ref('ephemeral_copy')}}
group by gender
order by gender asc
"""

incremental_copy_sql = """
{{
  config(
    materialized = "incremental"
  )
}}

select * from {{ this.schema }}.seed

{% if is_incremental() %}

    where id > (select max(id) from {{this}})

{% endif %}
"""

incremental_summary_sql = """
{{
  config(
    materialized = "table",
  )
}}

select gender, count(*) as ct from {{ref('incremental_copy')}}
group by gender
order by gender asc
"""

materialized_copy_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ this.schema }}.seed
"""

materialized_summary_sql = """
{{
  config(
    materialized = "table"
  )
}}

select gender, count(*) as ct from {{ref('materialized_copy')}}
group by gender
order by gender asc
"""

view_copy_sql = """
{{
  config(
    materialized = "view"
  )
}}

select * from {{ this.schema }}.seed
"""

view_summary_sql = """
{{
  config(
    materialized = "view"
  )
}}

select gender, count(*) as ct from {{ref('view_copy')}}
group by gender
order by gender asc
"""

view_using_ref_sql = """
{{
  config(
    materialized = "view"
  )
}}

select gender, count(*) as ct from {{ var('var_ref') }}
group by gender
order by gender asc
"""


@pytest.fixture
def models():
    return {
        'ephemeral_copy.sql': ephemeral_copy_sql,
        'ephemeral_summary.sql': ephemeral_summary_sql,
        'incremental_copy.sql': incremental_copy_sql,
        'incremental_summary.sql': incremental_summary_sql,
        'materialized_copy.sql': materialized_copy_sql,
        'materialized_summary.sql': materialized_summary_sql,
        'view_copy.sql': view_copy_sql,
        'view_summary.sql': view_summary_sql,
        'view_using_ref.sql': view_using_ref_sql,
    }


@pytest.fixture
def project_config_update():
    return {
        'vars': {
            'test': {
                'var_ref': '{{ ref("view_copy") }}',
            },
        },
    }


@pytest.fixture
def create_tables(test_data_dir, unique_schema):
    path = os.path.join(test_data_dir, 'seed.sql')
    run_sql_file(path, unique_schema)


# This test checks that with different materializations we get the right
# tables copied or built.
def test_simple_reference(project, create_tables):

    # Now run dbt
    results = run_dbt()
    assert len(results) == 8

    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )

    # Copies should match
    table_comp.assert_tables_equal("seed", "incremental_copy")
    table_comp.assert_tables_equal("seed", "materialized_copy")
    table_comp.assert_tables_equal("seed", "view_copy")

    # Summaries should match
    table_comp.assert_tables_equal("summary_expected", "incremental_summary")
    table_comp.assert_tables_equal("summary_expected", "materialized_summary")
    table_comp.assert_tables_equal("summary_expected", "view_summary")
    table_comp.assert_tables_equal("summary_expected", "ephemeral_summary")
    table_comp.assert_tables_equal("summary_expected", "view_using_ref")

    path = os.path.join(project.test_data_dir, 'update.sql')
    run_sql_file(path, project.test_schema)

    results = run_dbt()
    assert len(results) == 8

    # Copies should match
    table_comp.assert_tables_equal("seed", "incremental_copy")
    table_comp.assert_tables_equal("seed", "materialized_copy")
    table_comp.assert_tables_equal("seed", "view_copy")

    # Summaries should match
    table_comp.assert_tables_equal("summary_expected", "incremental_summary")
    table_comp.assert_tables_equal("summary_expected", "materialized_summary")
    table_comp.assert_tables_equal("summary_expected", "view_summary")
    table_comp.assert_tables_equal("summary_expected", "ephemeral_summary")


def test_simple_reference_with_models(project, create_tables):

    # Run materialized_copy, ephemeral_copy, and their dependents
    # ephemeral_copy should not actually be materialized b/c it is ephemeral
    results = run_dbt(['run', '--models', 'materialized_copy', 'ephemeral_copy'])
    assert len(results) == 1

    # Copies should match
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("seed", "materialized_copy")

    created_tables = get_tables_in_schema(project.test_schema)
    assert 'materialized_copy' in created_tables


def test_simple_reference_with_models_and_children(project, create_tables):

    # Run materialized_copy, ephemeral_copy, and their dependents
    results = run_dbt(['run', '--models', 'materialized_copy+', 'ephemeral_copy+'])
    assert len(results) == 3

    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )

    # Copies should match
    table_comp.assert_tables_equal("seed", "materialized_copy")

    # Summaries should match
    table_comp.assert_tables_equal("summary_expected", "materialized_summary")
    table_comp.assert_tables_equal("summary_expected", "ephemeral_summary")

    created_tables = get_tables_in_schema(project.test_schema)

    assert 'incremental_copy' not in created_tables
    assert 'incremental_summary' not in created_tables
    assert 'view_copy' not in created_tables
    assert 'view_summary' not in created_tables

    # make sure this wasn't errantly materialized
    assert 'ephemeral_copy' not in created_tables

    assert 'materialized_copy' in created_tables
    assert 'materialized_summary' in created_tables
    assert created_tables['materialized_copy'] == 'table'
    assert created_tables['materialized_summary'] == 'table'

    assert 'ephemeral_summary' in created_tables
    assert created_tables['ephemeral_summary'] == 'table'
