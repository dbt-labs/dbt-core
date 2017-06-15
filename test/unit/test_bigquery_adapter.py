import unittest
from mock import patch

import dbt.flags as flags

from dbt.adapters.bigquery import BigQueryAdapter
import dbt.exceptions
from dbt.logger import GLOBAL_LOGGER as logger  # noqa

fake_conn = {"handle": None, "state": "open", "type": "bigquery"}

class TestBigQueryAdapter(unittest.TestCase):

    def setUp(self):
        flags.STRICT_MODE = True

        self.oauth_profile = {
            "type": "bigquery",
            "method": "oauth",
            "project": 'dbt-unit-000000',
            "schema": "dummy_schema",
        }

        self.service_account_profile = {
            "type": "bigquery",
            "method": "service-account",
            "project": 'dbt-unit-000000',
            "schema": "dummy_schema",
            "keyfile": "/tmp/dummy-service-account.json",
        }

    @patch('dbt.adapters.bigquery.BigQueryAdapter.open_connection', return_value=fake_conn)
    def test_acquire_connection_oauth_validations(self, mock_open_connection):
        try:
            connection = BigQueryAdapter.acquire_connection(self.oauth_profile, 'dummy')
            self.assertEquals(connection.get('type'), 'bigquery')

        except dbt.exceptions.ValidationException as e:
            self.fail('got ValidationException: {}'.format(str(e)))

        except BaseException as e:
            raise
            self.fail('validation failed with unknown exception: {}'.format(str(e)))

        mock_open_connection.assert_called_once()

    @patch('dbt.adapters.bigquery.BigQueryAdapter.open_connection', return_value=fake_conn)
    def test_acquire_connection_service_account_validations(self, mock_open_connection):
        try:
            connection = BigQueryAdapter.acquire_connection(self.service_account_profile, 'dummy')
            self.assertEquals(connection.get('type'), 'bigquery')

        except dbt.exceptions.ValidationException as e:
            self.fail('got ValidationException: {}'.format(str(e)))

        except BaseException as e:
            raise
            self.fail('validation failed with unknown exception: {}'.format(str(e)))

        mock_open_connection.assert_called_once()
