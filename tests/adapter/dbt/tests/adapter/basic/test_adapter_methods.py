import pytest

from dbt.tests.util import run_dbt
from dbt.tests.fixtures.project import write_project_files
from dbt.tests.tables import TableComparison


tests__get_columns_in_relation_sql = """
{% set columns = adapter.get_columns_in_relation(ref('model')) %}
{% set limit_query = 0 %}
{% if (columns | length) == 0 %}
    {% set limit_query = 1 %}
{% endif %}

select 1 as id limit {{ limit_query }}

"""

models__upstream_sql = """
select 1 as id

"""

models__expected_sql = """
-- make sure this runs after 'model'
-- {{ ref('model') }}
select 2 as id

"""

models__model_sql = """

{% set upstream = ref('upstream') %}

{% if execute %}
    {# don't ever do any of this #}
    {%- do adapter.drop_schema(upstream) -%}
    {% set existing = adapter.get_relation(upstream.database, upstream.schema, upstream.identifier) %}
    {% if existing is not none %}
        {% do exceptions.raise_compiler_error('expected ' ~ ' to not exist, but it did') %}
    {% endif %}

    {%- do adapter.create_schema(upstream) -%}

    {% set sql = create_view_as(upstream, 'select 2 as id') %}
    {% do run_query(sql) %}
{% endif %}


select * from {{ upstream }}

"""


@pytest.fixture(scope="class")
def tests():
    return {"get_columns_in_relation.sql": tests__get_columns_in_relation_sql}


@pytest.fixture(scope="class")
def models():
    return {
        "upstream.sql": models__upstream_sql,
        "expected.sql": models__expected_sql,
        "model.sql": models__model_sql,
    }


@pytest.fixture(scope="class")
def project_files(
    project_root,
    tests,
    models,
):
    write_project_files(project_root, "tests", tests)
    write_project_files(project_root, "models", models)


class BaseCaching:
    @pytest.fixture(scope="class")
    def model_path(self):
        return "models"

    # snowflake need all tables in CAP name
    @pytest.fixture(scope="class")
    def equal_tables(self):
        return ["model", "expected"]

    def test_adapter_methods(self, project, equal_tables):
        run_dbt(["compile"])  # trigger any compile-time issues
        run_dbt()
        table_comp = TableComparison(
            adapter=project.adapter, unique_schema=project.test_schema, database=project.database
        )

        table_comp.assert_tables_equal(*equal_tables)


class TestBaseCaching(BaseCaching):
    pass
