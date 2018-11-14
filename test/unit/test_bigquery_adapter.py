import unittest
from mock import patch, MagicMock

import dbt.flags as flags

from dbt.adapters.bigquery import BigQueryCredentials
from dbt.adapters.bigquery import BigQueryAdapter
from dbt.adapters.bigquery import BigQueryRelation
import dbt.exceptions
from dbt.logger import GLOBAL_LOGGER as logger  # noqa

from .utils import config_from_parts_or_dicts


def _bq_conn():
    conn = MagicMock()
    conn.get.side_effect = lambda x: 'bigquery' if x == 'type' else None
    return conn


class TestBigQueryAdapter(unittest.TestCase):

    def setUp(self):
        flags.STRICT_MODE = True

        self.raw_profile = {
            'outputs': {
                'oauth': {
                    'type': 'bigquery',
                    'method': 'oauth',
                    'project': 'dbt-unit-000000',
                    'schema': 'dummy_schema',
                    'threads': 1,
                },
                'service_account': {
                    'type': 'bigquery',
                    'method': 'service-account',
                    'project': 'dbt-unit-000000',
                    'schema': 'dummy_schema',
                    'keyfile': '/tmp/dummy-service-account.json',
                    'threads': 1,
                },
            },
            'target': 'oauth',
        }

        self.project_cfg = {
            'name': 'X',
            'version': '0.1',
            'project-root': '/tmp/dbt/does-not-exist',
        }

    def get_adapter(self, profile):
        project = self.project_cfg.copy()
        project['profile'] = profile

        config = config_from_parts_or_dicts(
            project=project,
            profile=self.raw_profile,
        )
        return BigQueryAdapter(config)


    @patch('dbt.adapters.bigquery.BigQueryConnectionManager.open', return_value=_bq_conn())
    def test_acquire_connection_oauth_validations(self, mock_open_connection):
        adapter = self.get_adapter('oauth')
        try:
            connection = adapter.acquire_connection('dummy')
            self.assertEquals(connection.get('type'), 'bigquery')

        except dbt.exceptions.ValidationException as e:
            self.fail('got ValidationException: {}'.format(str(e)))

        except BaseException as e:
            raise
            self.fail('validation failed with unknown exception: {}'.format(str(e)))

        mock_open_connection.assert_called_once()

    @patch('dbt.adapters.bigquery.BigQueryConnectionManager.open', return_value=_bq_conn())
    def test_acquire_connection_service_account_validations(self, mock_open_connection):
        adapter = self.get_adapter('service_account')
        try:
            connection = adapter.acquire_connection('dummy')
            self.assertEquals(connection.get('type'), 'bigquery')

        except dbt.exceptions.ValidationException as e:
            self.fail('got ValidationException: {}'.format(str(e)))

        except BaseException as e:
            raise
            self.fail('validation failed with unknown exception: {}'.format(str(e)))

        mock_open_connection.assert_called_once()

    def test_cancel_open_connections_empty(self):
        adapter = self.get_adapter('test')
        self.assertEqual(adapter.cancel_open_connections(), None)

    def test_cancel_open_connections_master(self):
        adapter = self.get_adapter('test')
        adapter.connections.in_use['master'] = object()
        self.assertEqual(adapter.cancel_open_connections(), None)

    def test_cancel_open_connections_single(self):
        adapter = self.get_adapter('test')
        adapter.connections.in_use.update({
            'master': object(),
            'model': object(),
        })
        # actually does nothing
        self.assertEqual(adapter.cancel_open_connections(), None)


class TestBigQueryRelation(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = True

    def test_view_temp_relation(self):
        kwargs = {
            'type': None,
            'path': {
                'project': 'test-project',
                'schema': 'test_schema',
                'identifier': 'my_view'
            },
            'table_name': 'my_view__dbt_tmp',
            'quote_policy': {
                'identifier': False
            }
        }
        BigQueryRelation(**kwargs)

    def test_view_relation(self):
        kwargs = {
            'type': 'view',
            'path': {
                'project': 'test-project',
                'schema': 'test_schema',
                'identifier': 'my_view'
            },
            'table_name': 'my_view',
            'quote_policy': {
                'identifier': True,
                'schema': True
            }
        }
        BigQueryRelation(**kwargs)

    def test_table_relation(self):
        kwargs = {
            'type': 'table',
            'path': {
                'project': 'test-project',
                'schema': 'test_schema',
                'identifier': 'generic_table'
            },
            'table_name': 'generic_table',
            'quote_policy': {
                'identifier': True,
                'schema': True
            }
        }
        BigQueryRelation(**kwargs)

    def test_external_source_relation(self):
        kwargs = {
            'type': 'external',
            'path': {
                'project': 'test-project',
                'schema': 'test_schema',
                'identifier': 'sheet'
            },
            'table_name': 'sheet',
            'quote_policy': {
                'identifier': True,
                'schema': True
            }
        }
        BigQueryRelation(**kwargs)

    def test_invalid_relation(self):
        kwargs = {
            'type': 'invalid-type',
            'path': {
                'project': 'test-project',
                'schema': 'test_schema',
                'identifier': 'my_invalid_id'
            },
            'table_name': 'my_invalid_id',
            'quote_policy': {
                'identifier': False,
                'schema': True
            }
        }
        with self.assertRaises(dbt.exceptions.ValidationException):
            BigQueryRelation(**kwargs)
