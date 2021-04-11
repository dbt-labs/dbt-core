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

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seeds': {
                'quote_columns': False,
                'indexes': [
                  {'columns': ['country_code'], 'unique': False, 'type': 'hash'},
                  {'columns': ['country_code', 'country_name'], 'unique': True},
                ],
            },
            'vars': {
                'version': 1
            },
        }

    @use_profile('postgres')
    def test__postgres__table(self):
        results = self.run_dbt(['run', '--models', 'table'])
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

    @use_profile('postgres')
    def test__postgres__incremental(self):
        for additional_argument in [[], [], ['--full-refresh']]:
            results = self.run_dbt(['run', '--models', 'incremental'] + additional_argument)
            self.assertEqual(len(results),  1)

            indexes = self.get_indexes('incremental')
            self.assertCountEqual(
                indexes,
                [
                  {'columns': 'column_a', 'unique': False, 'type': 'hash'},
                  {'columns': 'column_a, column_b', 'unique': True, 'type': 'btree'},
                ]
            )

    @use_profile('postgres')
    def test__postgres__seed(self):
        for additional_argument in [[], [], ['--full-refresh']]:
            results = self.run_dbt(["seed"] + additional_argument)
            self.assertEqual(len(results),  1)

            indexes = self.get_indexes('seed')
            self.assertCountEqual(
                indexes,
                [
                  {'columns': 'country_code', 'unique': False, 'type': 'hash'},
                  {'columns': 'country_code, country_name', 'unique': True, 'type': 'btree'},
                ]
            )

    @use_profile('postgres')
    def test__postgres__snapshot(self):
        for version in [1, 2]:
            results = self.run_dbt(["snapshot", '--vars', 'version: {}'.format(version)])
            self.assertEqual(len(results),  1)

            indexes = self.get_indexes('colors')
            self.assertCountEqual(
                indexes,
                [
                  {'columns': 'id', 'unique': False, 'type': 'hash'},
                  {'columns': 'id, color', 'unique': True, 'type': 'btree'},
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

class TestPostgresInvalidIndex(DBTIntegrationTest):
    @property
    def schema(self):
        return "postgres_index_065"

    @property
    def models(self):
        return "models-invalid"

    @use_profile('postgres')
    def test__postgres__invalid_index_configs(self):
        results, output = self.run_dbt_and_capture(expect_pass=False)
        self.assertEqual(len(results), 4)
        self.assertRegex(output, r'columns.*is not of type \'array\'')
        self.assertRegex(output, r'unique.*is not of type \'boolean\'')
        self.assertRegex(output, r'\'columns\' is a required property')
        self.assertRegex(output, r'Database Error in model invalid_type')
