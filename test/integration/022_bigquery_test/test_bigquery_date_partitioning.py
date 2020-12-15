from test.integration.base import DBTIntegrationTest, use_profile
import textwrap
import yaml


class TestBigqueryDatePartitioning(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test_022"

    @property
    def models(self):
        return "dp-models"

    @property
    def profile_config(self):
        return self.bigquery_profile()

    @property
    def project_config(self):
        return yaml.safe_load(textwrap.dedent('''\
        config-version: 2
        models:
            test:
                partitioned_noconfig:
                    materialized: table
                    partitions:
                        - 20180101
                        - 20180102
                        - 20180103
                    verbose: true
        '''))

    @use_profile('bigquery')
    def test__bigquery_date_partitioning(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 8)

        test_results = self.run_dbt(['test'])

        self.assertTrue(len(test_results) > 0)
        for result in test_results:
            self.assertEqual(result.status, 'pass')
            self.assertFalse(result.skipped)
            # message = # of failing rows
            self.assertEqual(int(result.message), 0)
