from tests.integration.base import DBTIntegrationTest, use_profile


class TestColumnTypes(DBTIntegrationTest):
    @property
    def schema(self):
        return 'column_types'

    def run_and_test(self):
        self.assertEqual(len(self.run_dbt(['run'])), 1)
        self.assertEqual(len(self.run_dbt(['test'])), 1)


class TestBigQueryColumnTypes(TestColumnTypes):
    @property
    def models(self):
        return 'bq_models'

    @use_profile('bigquery')
    def test_bigquery_column_types(self):
        self.run_and_test()
