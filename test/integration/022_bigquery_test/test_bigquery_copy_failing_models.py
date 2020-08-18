from test.integration.base import DBTIntegrationTest, use_profile
import textwrap
import yaml


class TestBigqueryDatePartitioning(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test_022"

    @property
    def models(self):
        return "copy-failing-models"

    @property
    def profile_config(self):
        return self.bigquery_profile()

    @property
    def project_config(self):
        return yaml.safe_load(textwrap.dedent('''\
        config-version: 2
        models:
            test:
                copy_bad_materialization:
                    materialized: view
        '''))

    @use_profile('bigquery')
    def test__bigquery_copy_table(self):
        results = self.run_dbt(expect_pass=False)
        # Copy SQL macro raises a NotImplementedException, which gives None
        # as results.
        self.assertEqual(results, None)
