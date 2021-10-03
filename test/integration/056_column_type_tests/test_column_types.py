from test.integration.base import DBTIntegrationTest, use_profile


class TestColumnTypes(DBTIntegrationTest):
    @property
    def schema(self):
        return '056_column_types'

    def run_and_test(self):
        self.assertEqual(len(self.run_dbt(['run'])), 1)
        self.assertEqual(len(self.run_dbt(['test'])), 1)


class TestPostgresColumnTypes(TestColumnTypes):
    @property
    def models(self):
        return 'pg_models'

    @use_profile('postgres')
    def test_postgres_column_types(self):
        self.run_and_test()

