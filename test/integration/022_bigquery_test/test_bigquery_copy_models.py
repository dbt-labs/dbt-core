from test.integration.base import DBTIntegrationTest, use_profile
import textwrap
import yaml


class TestBigqueryDatePartitioning(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test_022"

    @property
    def models(self):
        return "copy-models"

    @property
    def profile_config(self):
        return self.bigquery_profile()

    @property
    def project_config(self):
        return yaml.safe_load(textwrap.dedent('''\
        config-version: 2
        models:
            test:
                original:
                    materialized: table
                copy_as_table:
                    materialized: table
                copy_as_incremental:
                    materialized: incremental
                copy_bad_materialization:
                    materialized: view
        '''))

    @use_profile('bigquery')
    def test__bigquery_copy_table(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 4)

        test_results = self.run_dbt(['test'])
        print('test_results is', test_results)

        self.assertTrue(len(test_results) > 0)
        for result in test_results:
            if result.name == 'copy_bad_materialization':
                self.assertTrue(result.error)
            else:
                self.assertTrue(result.success)

