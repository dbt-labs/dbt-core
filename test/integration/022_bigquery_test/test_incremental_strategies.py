from test.integration.base import DBTIntegrationTest, FakeArgs, use_profile

class TestBigQueryScripting(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test_022"

    @property
    def models(self):
        return "incremental-strategy-models"

    @property
    def project_config(self):
        return {
            "config_version": 2,
            "seeds": {
                "+quote_columns": False
            },
            "models": {
                "require_partition_filter": True
            }
        }

    @use_profile('bigquery')
    def test__bigquery_assert_incrementals(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 7)

        results = self.run_dbt()
        self.assertEqual(len(results), 7)

        results = self.run_dbt(['seed'])

        self.assertTablesEqual('incremental_merge_range', 'merge_expected')
        self.assertTablesEqual('incremental_merge_time', 'merge_expected')
        self.assertTablesEqual('incremental_overwrite_time', 'incremental_overwrite_time_expected')
        self.assertTablesEqual('incremental_overwrite_date', 'incremental_overwrite_date_expected')
        self.assertTablesEqual('incremental_overwrite_partitions', 'incremental_overwrite_date_expected')
        self.assertTablesEqual('incremental_overwrite_day', 'incremental_overwrite_day_expected')
        self.assertTablesEqual('incremental_overwrite_range', 'incremental_overwrite_range_expected')
