from test.integration.base import DBTIntegrationTest, FakeArgs, use_profile


class TestBigqueryDatePartitioning(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test_022"

    @property
    def models(self):
        return "test/integration/022_bigquery_test/dp-models"

    @property
    def profile_config(self):
        return self.bigquery_profile()

    @use_profile('bigquery')
    def test__bigquery_date_partitioning(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 6)

        test_results = self.run_dbt(['test'])

        self.assertTrue(len(test_results) > 0)
        for result in test_results:
            self.assertIsNone(result.error)
            self.assertFalse(result.skipped)
            # status = # of failing rows
            self.assertEqual(result.status, 0)
