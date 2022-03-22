import pytest
from dbt.tests.util import run_dbt, copy_file, read_file
from dbt.tests.tables import TableComparison


ephemeral_copy_sql = """
{{
  config(
    materialized = "ephemeral"
  )
}}

select * from {{ this.schema }}.users
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

select * from {{ this.schema }}.users

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

select * from {{ this.schema }}.users
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

select * from {{ this.schema }}.users
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

properties_yml = """
version: 2
seeds:
  - name: summary_expected
    config:
      column_types:
        ct: BIGINT
        gender: text
"""


@pytest.fixture(scope="class")
def models():
    return {
        "ephemeral_copy.sql": ephemeral_copy_sql,
        "ephemeral_summary.sql": ephemeral_summary_sql,
        "incremental_copy.sql": incremental_copy_sql,
        "incremental_summary.sql": incremental_summary_sql,
        "materialized_copy.sql": materialized_copy_sql,
        "materialized_summary.sql": materialized_summary_sql,
        "view_copy.sql": view_copy_sql,
        "view_summary.sql": view_summary_sql,
        "view_using_ref.sql": view_using_ref_sql,
    }


@pytest.fixture(scope="class")
def seeds(test_data_dir):
    # Read seed file and return
    seeds = {"properties.yml": properties_yml}
    seed_csv = read_file(test_data_dir, "seed-initial.csv")
    seeds["users.csv"] = seed_csv
    summary_csv = read_file(test_data_dir, "summary_expected.csv")
    seeds["summary_expected.csv"] = summary_csv
    return seeds


@pytest.fixture(scope="class")
def project_config_update():
    return {
        "vars": {
            "test": {
                "var_ref": '{{ ref("view_copy") }}',
            },
        },
        "seeds": {"quote_columns": False},
    }


# This test checks that with different materializations we get the right
# tables copied or built.
def test_simple_reference(project):
    results = run_dbt(["seed"])
    assert len(results) == 2

    # Now run dbt
    results = run_dbt()
    assert len(results) == 8

    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )

    # Copies should match
    table_comp.assert_tables_equal("users", "incremental_copy")
    table_comp.assert_tables_equal("users", "materialized_copy")
    table_comp.assert_tables_equal("users", "view_copy")

    # Summaries should match
    table_comp.assert_tables_equal("summary_expected", "incremental_summary")
    table_comp.assert_tables_equal("summary_expected", "materialized_summary")
    table_comp.assert_tables_equal("summary_expected", "view_summary")
    table_comp.assert_tables_equal("summary_expected", "ephemeral_summary")
    table_comp.assert_tables_equal("summary_expected", "view_using_ref")

    # update the seed files and run seed
    copy_file(
        project.test_data_dir, "seed-update.csv", project.project_root, ["seeds", "users.csv"]
    )
    copy_file(
        project.test_data_dir,
        "summary_expected_update.csv",
        project.project_root,
        ["seeds", "summary_expected.csv"],
    )
    results = run_dbt(["seed"])
    assert len(results) == 2

    results = run_dbt()
    assert len(results) == 8

    # Copies should match
    table_comp.assert_tables_equal("users", "incremental_copy")
    table_comp.assert_tables_equal("users", "materialized_copy")
    table_comp.assert_tables_equal("users", "view_copy")

    # Summaries should match
    table_comp.assert_tables_equal("summary_expected", "incremental_summary")
    table_comp.assert_tables_equal("summary_expected", "materialized_summary")
    table_comp.assert_tables_equal("summary_expected", "view_summary")
    table_comp.assert_tables_equal("summary_expected", "ephemeral_summary")


def test_simple_reference_with_models_and_children(project):
    results = run_dbt(["seed"])
    assert len(results) == 2

    # Run materialized_copy, ephemeral_copy, and their dependents
    results = run_dbt(["run", "--models", "materialized_copy+", "ephemeral_copy+"])
    assert len(results) == 3

    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )

    # Copies should match
    table_comp.assert_tables_equal("users", "materialized_copy")

    # Summaries should match
    table_comp.assert_tables_equal("summary_expected", "materialized_summary")
    table_comp.assert_tables_equal("summary_expected", "ephemeral_summary")

    created_tables = project.get_tables_in_schema()

    assert "incremental_copy" not in created_tables
    assert "incremental_summary" not in created_tables
    assert "view_copy" not in created_tables
    assert "view_summary" not in created_tables

    # make sure this wasn't errantly materialized
    assert "ephemeral_copy" not in created_tables

    assert "materialized_copy" in created_tables
    assert "materialized_summary" in created_tables
    assert created_tables["materialized_copy"] == "table"
    assert created_tables["materialized_summary"] == "table"

    assert "ephemeral_summary" in created_tables
    assert created_tables["ephemeral_summary"] == "table"


def test_simple_ref_with_models(project):
    results = run_dbt(["seed"])
    assert len(results) == 2

    # Run materialized_copy, ephemeral_copy, and their dependents
    # ephemeral_copy should not actually be materialized b/c it is ephemeral
    results = run_dbt(["run", "--models", "materialized_copy", "ephemeral_copy"])
    assert len(results) == 1

    # Copies should match
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("users", "materialized_copy")

    created_tables = project.get_tables_in_schema()
    assert "materialized_copy" in created_tables
