import pytest

from dbt.tests.util import run_dbt

schema_yml = """
version: 2
models:
  - name: table_unlogged
    description: "Unlogged table model"
    columns:
      - name: column_a
        description: "Sample description"
        quote: true
"""

table_unlogged_sql = """
{{ config(materialized = 'table', unlogged = True) }}

select 1 as column_a
"""


class TestPostgresUnloggedTable:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "table_unlogged.sql": table_unlogged_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "materialized": "table",
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }

    def test_postgres_unlogged_table_catalog(self, project):
        table_name = "table_unlogged"

        results = run_dbt(["run", "--models", table_name])
        assert len(results) == 1

        result = self.get_table_persistence(project, table_name)
        assert result == "u"

        catalog = run_dbt(["docs", "generate"])

        assert len(catalog.nodes) == 1

        table_node = catalog.nodes["model.test.table_unlogged"]
        assert table_node
        assert "column_a" in table_node.columns

    def get_table_persistence(self, project, table_name):
        sql = """
            SELECT
              relpersistence
            FROM pg_class
            WHERE relname = '{table_name}'
        """
        sql = sql.format(table_name=table_name, schema=project.test_schema)
        result = project.run_sql(sql, fetch="one")
        assert len(result) == 1

        return result[0]
