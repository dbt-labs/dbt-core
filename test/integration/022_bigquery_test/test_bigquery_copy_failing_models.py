from test.integration.base import DBTIntegrationTest, use_profile
import textwrap
import yaml


class TestBigqueryCopyTableFails(DBTIntegrationTest):

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
                original:
                    materialized: table
                copy_bad_materialization:
                    materialized: copy
        '''))

    @use_profile('bigquery')
    def test__bigquery_copy_table_fails(self):
        results = self.run_dbt(expect_pass=False)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[1].status, 'error')
