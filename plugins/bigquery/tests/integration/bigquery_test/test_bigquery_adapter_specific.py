""""Test adapter specific config options."""
from pprint import pprint

from tests.integration.base import DBTIntegrationTest, use_profile
import textwrap
import yaml


class TestBigqueryAdapterSpecific(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test"

    @property
    def models(self):
        return "adapter-specific-models"

    @property
    def profile_config(self):
        return self.bigquery_profile()

    @property
    def project_config(self):
        return yaml.safe_load(textwrap.dedent('''\
        config-version: 2
        models:
            test:
                materialized: table
                expiring_table:
                    hours_to_expiration: 4    
        '''))

    @use_profile('bigquery')
    def test_bigquery_hours_to_expiration(self):
        _, stdout = self.run_dbt_and_capture(['--debug', 'run'])

        self.assertIn(
            'expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL '
            '4 hour)', stdout)
