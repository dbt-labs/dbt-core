from test.integration.base import DBTIntegrationTest, FakeArgs, use_profile

class TestBigQueryScripting(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test_022"

    @property
    def models(self):
        return "scripting-models"

    @property
    def profile_config(self):
        return self.bigquery_profile()

    def assert_incrementals(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        self.run_dbt()
        self.assertEqual(len(results), 3)

        results = self.run_dbt(['seed'])

        self.assertTablesEqual('incremental_overwrite', 'incremental_overwrite_expected')
