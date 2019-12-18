import unittest
from contextlib import contextmanager
from unittest.mock import patch, MagicMock, Mock

import hologram

import dbt.flags as flags

from dbt.adapters.bigquery import BigQueryCredentials
from dbt.adapters.bigquery import BigQueryAdapter
from dbt.adapters.bigquery import BigQueryRelation
from dbt.adapters.bigquery.connections import BigQueryConnectionManager
import dbt.exceptions
from dbt.logger import GLOBAL_LOGGER as logger  # noqa

from .utils import config_from_parts_or_dicts, inject_adapter


def _bq_conn():
    conn = MagicMock()
    conn.get.side_effect = lambda x: 'bigquery' if x == 'type' else None
    return conn


class BaseTestBigQueryAdapter(unittest.TestCase):

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
                'loc': {
                    'type': 'bigquery',
                    'method': 'oauth',
                    'project': 'dbt-unit-000000',
                    'schema': 'dummy_schema',
                    'threads': 1,
                    'location': 'Luna Station',
                    'priority': 'batch',
                },
            },
            'target': 'oauth',
        }

        self.project_cfg = {
            'name': 'X',
            'version': '0.1',
            'project-root': '/tmp/dbt/does-not-exist',
            'profile': 'default',
        }
        self.qh_patch = None

    def tearDown(self):
        if self.qh_patch:
            self.qh_patch.stop()
        super().tearDown()

    def get_adapter(self, target):
        project = self.project_cfg.copy()
        profile = self.raw_profile.copy()
        profile['target'] = target

        config = config_from_parts_or_dicts(
            project=project,
            profile=profile,
        )
        adapter = BigQueryAdapter(config)

        self.qh_patch = patch.object(adapter.connections.query_header, 'add')
        self.mock_query_header_add = self.qh_patch.start()
        self.mock_query_header_add.side_effect = lambda q: '/* dbt */\n{}'.format(q)

        inject_adapter(adapter)
        return adapter


class TestBigQueryAdapterAcquire(BaseTestBigQueryAdapter):
    @patch('dbt.adapters.bigquery.BigQueryConnectionManager.open', return_value=_bq_conn())
    def test_acquire_connection_oauth_validations(self, mock_open_connection):
        adapter = self.get_adapter('oauth')
        try:
            connection = adapter.acquire_connection('dummy')
            self.assertEqual(connection.type, 'bigquery')

        except dbt.exceptions.ValidationException as e:
            self.fail('got ValidationException: {}'.format(str(e)))

        except BaseException as e:
            raise

        mock_open_connection.assert_not_called()
        connection.handle
        mock_open_connection.assert_called_once()

    @patch('dbt.adapters.bigquery.BigQueryConnectionManager.open', return_value=_bq_conn())
    def test_acquire_connection_service_account_validations(self, mock_open_connection):
        adapter = self.get_adapter('service_account')
        try:
            connection = adapter.acquire_connection('dummy')
            self.assertEqual(connection.type, 'bigquery')

        except dbt.exceptions.ValidationException as e:
            self.fail('got ValidationException: {}'.format(str(e)))

        except BaseException as e:
            raise

        mock_open_connection.assert_not_called()
        connection.handle
        mock_open_connection.assert_called_once()

    @patch('dbt.adapters.bigquery.BigQueryConnectionManager.open', return_value=_bq_conn())
    def test_acquire_connection_priority(self, mock_open_connection):
        adapter = self.get_adapter('loc')
        try:
            connection = adapter.acquire_connection('dummy')
            self.assertEqual(connection.type, 'bigquery')
            self.assertEqual(connection.credentials.priority, 'batch')

        except dbt.exceptions.ValidationException as e:
            self.fail('got ValidationException: {}'.format(str(e)))

        mock_open_connection.assert_not_called()
        connection.handle
        mock_open_connection.assert_called_once()

    def test_cancel_open_connections_empty(self):
        adapter = self.get_adapter('oauth')
        self.assertEqual(adapter.cancel_open_connections(), None)

    def test_cancel_open_connections_master(self):
        adapter = self.get_adapter('oauth')
        adapter.connections.thread_connections[0] = object()
        self.assertEqual(adapter.cancel_open_connections(), None)

    def test_cancel_open_connections_single(self):
        adapter = self.get_adapter('oauth')
        adapter.connections.thread_connections.update({
            0: object(),
            1: object(),
        })
        # actually does nothing
        self.assertEqual(adapter.cancel_open_connections(), None)

    @patch('dbt.adapters.bigquery.impl.google.auth.default')
    @patch('dbt.adapters.bigquery.impl.google.cloud.bigquery')
    def test_location_value(self, mock_bq, mock_auth_default):
        creds = MagicMock()
        mock_auth_default.return_value = (creds, MagicMock())
        adapter = self.get_adapter('loc')

        connection = adapter.acquire_connection('dummy')
        mock_client = mock_bq.Client

        mock_client.assert_not_called()
        connection.handle
        mock_client.assert_called_once_with('dbt-unit-000000', creds,
                                            location='Luna Station')


class TestConnectionNamePassthrough(BaseTestBigQueryAdapter):

    def setUp(self):
        super().setUp()
        self._conn_patch = patch.object(BigQueryAdapter, 'ConnectionManager')
        self.conn_manager_cls = self._conn_patch.start()

        self._relation_patch = patch.object(BigQueryAdapter, 'Relation')
        self.relation_cls = self._relation_patch.start()

        self.mock_connection_manager = self.conn_manager_cls.return_value
        self.conn_manager_cls.TYPE = 'bigquery'
        self.relation_cls.get_default_quote_policy.side_effect = BigQueryRelation.get_default_quote_policy

        self.adapter = self.get_adapter('oauth')

    def tearDown(self):
        super().tearDown()
        self._conn_patch.stop()
        self._relation_patch.stop()

    def test_get_relation(self):
        self.adapter.get_relation('db', 'schema', 'my_model')
        self.mock_connection_manager.get_bq_table.assert_called_once_with('db', 'schema', 'my_model')

    def test_create_schema(self):
        self.adapter.create_schema('db', 'schema')
        self.mock_connection_manager.create_dataset.assert_called_once_with('db', 'schema')

    @patch.object(BigQueryAdapter, 'check_schema_exists')
    def test_drop_schema(self, mock_check_schema):
        mock_check_schema.return_value = True
        self.adapter.drop_schema('db', 'schema')
        self.mock_connection_manager.drop_dataset.assert_called_once_with('db', 'schema')

    def test_get_columns_in_relation(self):
        self.mock_connection_manager.get_bq_table.side_effect = ValueError
        self.adapter.get_columns_in_relation(
            MagicMock(database='db', schema='schema', identifier='ident'),
        )
        self.mock_connection_manager.get_bq_table.assert_called_once_with(
            database='db', schema='schema', identifier='ident'
        )


class TestBigQueryRelation(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = True

    def test_view_temp_relation(self):
        kwargs = {
            'type': None,
            'path': {
                'database': 'test-project',
                'schema': 'test_schema',
                'identifier': 'my_view'
            },
            'quote_policy': {
                'identifier': False
            }
        }
        BigQueryRelation.from_dict(kwargs)

    def test_view_relation(self):
        kwargs = {
            'type': 'view',
            'path': {
                'database': 'test-project',
                'schema': 'test_schema',
                'identifier': 'my_view'
            },
            'quote_policy': {
                'identifier': True,
                'schema': True
            }
        }
        BigQueryRelation.from_dict(kwargs)

    def test_table_relation(self):
        kwargs = {
            'type': 'table',
            'path': {
                'database': 'test-project',
                'schema': 'test_schema',
                'identifier': 'generic_table'
            },
            'quote_policy': {
                'identifier': True,
                'schema': True
            }
        }
        BigQueryRelation.from_dict(kwargs)

    def test_external_source_relation(self):
        kwargs = {
            'type': 'external',
            'path': {
                'database': 'test-project',
                'schema': 'test_schema',
                'identifier': 'sheet'
            },
            'quote_policy': {
                'identifier': True,
                'schema': True
            }
        }
        BigQueryRelation.from_dict(kwargs)

    def test_invalid_relation(self):
        kwargs = {
            'type': 'invalid-type',
            'path': {
                'database': 'test-project',
                'schema': 'test_schema',
                'identifier': 'my_invalid_id'
            },
            'quote_policy': {
                'identifier': False,
                'schema': True
            }
        }
        with self.assertRaises(hologram.ValidationError):
            BigQueryRelation.from_dict(kwargs)


class TestBigQueryConnectionManager(unittest.TestCase):

    def setUp(self):
        credentials = Mock(BigQueryCredentials)
        profile = Mock(query_comment=None, credentials=credentials)
        self.connections = BigQueryConnectionManager(profile=profile)
        self.mock_client = Mock(
          dbt.adapters.bigquery.impl.google.cloud.bigquery.Client)
        self.mock_connection = MagicMock()

        self.mock_connection.handle = self.mock_client

        self.connections.get_thread_connection = lambda: self.mock_connection

    def test_retry_and_handle(self):
        self.connections.DEFAULT_MAXIMUM_DELAY = 2.0
        dbt.adapters.bigquery.connections._is_retryable = lambda x: True

        @contextmanager
        def dummy_handler(msg):
            yield

        self.connections.exception_handler = dummy_handler

        class DummyException(Exception):
            """Count how many times this exception is raised"""
            count = 0

            def __init__(self):
                DummyException.count += 1

        def raiseDummyException():
            raise DummyException()

        with self.assertRaises(DummyException):
            self.connections._retry_and_handle(
                 "some sql", Mock(credentials=Mock(retries=8)),
                 raiseDummyException)
            self.assertEqual(DummyException.count, 9)

    def test_is_retryable(self):
        _is_retryable = dbt.adapters.bigquery.connections._is_retryable
        exceptions = dbt.adapters.bigquery.impl.google.cloud.exceptions
        internal_server_error = exceptions.InternalServerError('code broke')
        bad_request_error = exceptions.BadRequest('code broke')

        self.assertTrue(_is_retryable(internal_server_error))
        self.assertFalse(_is_retryable(bad_request_error))

    def test_drop_dataset(self):
        mock_table = Mock()
        mock_table.reference = 'table1'

        self.mock_client.list_tables.return_value = [mock_table]

        self.connections.drop_dataset('project', 'dataset')

        self.mock_client.list_tables.assert_not_called()
        self.mock_client.delete_table.assert_not_called()
        self.mock_client.delete_dataset.assert_called_once()

    @patch('dbt.adapters.bigquery.impl.google.cloud.bigquery')
    def test_query_and_results(self, mock_bq):
        self.connections.get_timeout = lambda x: 100.0

        self.connections._query_and_results(
          self.mock_client, 'sql', self.mock_connection,
          {'description': 'blah'})

        mock_bq.QueryJobConfig.assert_called_once()
        self.mock_client.query.assert_called_once_with(
          'sql', job_config=mock_bq.QueryJobConfig())
