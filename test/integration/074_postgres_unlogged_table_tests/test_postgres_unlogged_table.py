import re
import json
from test.integration.base import DBTIntegrationTest, use_profile


class TestPostgresUnloggedTable(DBTIntegrationTest):
    @property
    def schema(self):
        return "postgres_unlogged_074"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'models': {
                'test': {
                    'materialized': 'table',
                    '+persist_docs': {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }

    @use_profile('postgres')
    def test__postgres__unlogged__table__catalog(self):
        table_name = 'table_unlogged'

        results = self.run_dbt(['run', '--models', table_name])
        self.assertEqual(len(results),  1)

        assert self.get_table_persistence(table_name) == 'u'

        self.run_dbt(['docs', 'generate'])

        with open('target/catalog.json') as fp:
            catalog_data = json.load(fp)

        assert len(catalog_data['nodes']) == 1

        table_node = catalog_data['nodes'][f'model.test.{table_name}']
        assert 'column_a' in table_node['columns']

    def get_table_persistence(self, table_name):
        sql = """
            SELECT
              relpersistence
            FROM pg_class
            WHERE relname = '{table_name}'
        """
        sql = sql.format(table_name=table_name, schema=self.unique_schema())

        result, = self.run_sql(sql, fetch='one')

        self.assertEqual(len(result),  1)

        return result
