import re

from test.integration.base import DBTIntegrationTest, use_profile


INDEX_DEFINITION_PATTERN = re.compile(r'using\s+(\w+)\s+\((.+)\)\Z')

class TestPostgresIndex(DBTIntegrationTest):
    @property
    def schema(self):
        return "postgres_index_065"

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test__postgres__table(self):
        results = self.run_dbt()
        self.assertEqual(len(results),  1)

        indexes = self.get_indexes('table')
        self.assertCountEqual(
            indexes,
            [
              {'columns': 'column_a', 'unique': False, 'type': 'btree'},
              {'columns': 'column_b', 'unique': False, 'type': 'btree'},
              {'columns': 'column_a, column_b', 'unique': False, 'type': 'btree'},
              {'columns': 'column_b, column_a', 'unique': True, 'type': 'btree'},
              {'columns': 'column_a', 'unique': False, 'type': 'hash'}
            ]
        )

    def get_indexes(self, table_name):
        sql = """
            SELECT
              pg_get_indexdef(idx.indexrelid) as index_definition
            FROM pg_index idx
            JOIN pg_class tab ON tab.oid = idx.indrelid
            WHERE
              tab.relname = '{table}'
              AND tab.relnamespace = (
                SELECT oid FROM pg_namespace WHERE nspname = '{schema}'
              );
        """

        sql = sql.format(table=table_name, schema=self.unique_schema())
        results = self.run_sql(sql, fetch='all')
        return [self.parse_index_definition(row[0]) for row in results]

    def parse_index_definition(self, index_definition):
        index_definition = index_definition.lower()
        is_unique = 'unique' in index_definition
        m = INDEX_DEFINITION_PATTERN.search(index_definition)
        return {'columns': m.group(2), 'unique': is_unique, 'type': m.group(1)}
