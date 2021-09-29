from tests.integration.base import DBTIntegrationTest, use_profile
import textwrap
import yaml


class TestBigqueryCopyTable(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test"

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
                additional:
                    materialized: table
                copy_as_table:
                    materialized: copy
                copy_as_several_tables:
                    materialized: copy
                copy_as_incremental:
                    materialized: copy
        '''))

    @use_profile('bigquery')
    def test__bigquery_copy_table(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 5)
