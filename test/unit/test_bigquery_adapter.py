import agate
import decimal
import re
import unittest
from contextlib import contextmanager
from requests.exceptions import ConnectionError
from unittest.mock import patch, MagicMock, Mock, create_autospec, ANY

import dbt.dataclass_schema

import dbt.flags as flags

from dbt.adapters.bigquery import BigQueryCredentials
from dbt.adapters.bigquery import BigQueryAdapter
from dbt.adapters.bigquery import BigQueryRelation
from dbt.adapters.bigquery import Plugin as BigQueryPlugin
from dbt.adapters.bigquery.connections import BigQueryConnectionManager
from dbt.adapters.base.query_headers import MacroQueryStringSetter
from dbt.clients import agate_helper
import dbt.exceptions
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from dbt.context.providers import RuntimeConfigObject

import google.cloud.bigquery

from .utils import config_from_parts_or_dicts, inject_adapter, TestAdapterConversions


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
                    'maximum_bytes_billed': 0,
                },
                'impersonate': {
                    'type': 'bigquery',
                    'method': 'oauth',
                    'project': 'dbt-unit-000000',
                    'schema': 'dummy_schema',
                    'threads': 1,
                    'impersonate_service_account': 'dummyaccount@dbt.iam.gserviceaccount.com'
                },
                'oauth-credentials-token': {
                    'type': 'bigquery',
                    'method': 'oauth-secrets',
                    'token': 'abc',
                    'project': 'dbt-unit-000000',
                    'schema': 'dummy_schema',
                    'threads': 1,
                    'location': 'Luna Station',
                    'priority': 'batch',
                    'maximum_bytes_billed': 0,
                },
                'oauth-credentials': {
                    'type': 'bigquery',
                    'method': 'oauth-secrets',
                    'client_id': 'abc',
                    'client_secret': 'def',
                    'refresh_token': 'ghi',
                    'token_uri': 'jkl',
                    'project': 'dbt-unit-000000',
                    'schema': 'dummy_schema',
                    'threads': 1,
                    'location': 'Luna Station',
                    'priority': 'batch',
                    'maximum_bytes_billed': 0,
                },
                'oauth--no-project': {
                    'type': 'bigquery',
                    'method': 'oauth',
                    'schema': 'dummy_schema',
                    'threads': 1,
                    'location': 'Solar Station',
                },
            },
            'target': 'oauth',
        }

        self.project_cfg = {
            'name': 'X',
            'version': '0.1',
            'project-root': '/tmp/dbt/does-not-exist',
            'profile': 'default',
            'config-version': 2,
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

        adapter.connections.query_header = MacroQueryStringSetter(config, MagicMock(macros={}))

        self.qh_patch = patch.object(adapter.connections.query_header, 'add')
        self.mock_query_header_add = self.qh_patch.start()
        self.mock_query_header_add.side_effect = lambda q: '/* dbt */\n{}'.format(q)

        inject_adapter(adapter, BigQueryPlugin)
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
    def test_acquire_connection_oauth_token_validations(self, mock_open_connection):
        adapter = self.get_adapter('oauth-credentials-token')
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
    def test_acquire_connection_oauth_credentials_validations(self, mock_open_connection):
        adapter = self.get_adapter('oauth-credentials')
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
    def test_acquire_connection_impersonated_service_account_validations(self, mock_open_connection):
        adapter = self.get_adapter('impersonate')
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

    @patch('dbt.adapters.bigquery.BigQueryConnectionManager.open', return_value=_bq_conn())
    def test_acquire_connection_maximum_bytes_billed(self, mock_open_connection):
        adapter = self.get_adapter('loc')
        try:
            connection = adapter.acquire_connection('dummy')
            self.assertEqual(connection.type, 'bigquery')
            self.assertEqual(connection.credentials.maximum_bytes_billed, 0)

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
    def test_location_user_agent(self, mock_bq, mock_auth_default):
        creds = MagicMock()
        mock_auth_default.return_value = (creds, MagicMock())
        adapter = self.get_adapter('loc')

        connection = adapter.acquire_connection('dummy')
        mock_client = mock_bq.Client

        mock_client.assert_not_called()
        connection.handle
        mock_client.assert_called_once_with('dbt-unit-000000', creds,
                                            location='Luna Station',
                                            client_info=HasUserAgent())


class HasUserAgent:
    PAT = re.compile(r'dbt-\d+\.\d+\.\d+((a|b|rc)\d+)?')

    def __eq__(self, other):
        compare = getattr(other, 'user_agent', '')
        return bool(self.PAT.match(compare))


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
        relation = BigQueryRelation.create(database='db', schema='schema')
        self.adapter.create_schema(relation)
        self.mock_connection_manager.create_dataset.assert_called_once_with('db', 'schema')

    @patch.object(BigQueryAdapter, 'check_schema_exists')
    def test_drop_schema(self, mock_check_schema):
        mock_check_schema.return_value = True
        relation = BigQueryRelation.create(database='db', schema='schema')
        self.adapter.drop_schema(relation)
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
        BigQueryRelation.validate(kwargs)

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
        BigQueryRelation.validate(kwargs)

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
        BigQueryRelation.validate(kwargs)

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
        BigQueryRelation.validate(kwargs)

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
        with self.assertRaises(dbt.dataclass_schema.ValidationError):
            BigQueryRelation.validate(kwargs)


class TestBigQueryInformationSchema(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = True

    def test_replace(self):

        kwargs = {
            'type': None,
            'path': {
                'database': 'test-project',
                'schema': 'test_schema',
                'identifier': 'my_view'
            },
            # test for #2188
            'quote_policy': {
                'database': False
            },
            'include_policy': {
                'database': True,
                'schema': True,
                'identifier': True,
            }
        }
        BigQueryRelation.validate(kwargs)
        relation = BigQueryRelation.from_dict(kwargs)
        info_schema = relation.information_schema()

        tables_schema = info_schema.replace(information_schema_view='__TABLES__')
        assert tables_schema.information_schema_view == '__TABLES__'
        assert tables_schema.include_policy.schema is True
        assert tables_schema.include_policy.identifier is False
        assert tables_schema.include_policy.database is True
        assert tables_schema.quote_policy.schema is True
        assert tables_schema.quote_policy.identifier is False
        assert tables_schema.quote_policy.database is False

        schemata_schema = info_schema.replace(information_schema_view='SCHEMATA')
        assert schemata_schema.information_schema_view == 'SCHEMATA'
        assert schemata_schema.include_policy.schema is False
        assert schemata_schema.include_policy.identifier is True
        assert schemata_schema.include_policy.database is True
        assert schemata_schema.quote_policy.schema is True
        assert schemata_schema.quote_policy.identifier is False
        assert schemata_schema.quote_policy.database is False

        other_schema = info_schema.replace(information_schema_view='SOMETHING_ELSE')
        assert other_schema.information_schema_view == 'SOMETHING_ELSE'
        assert other_schema.include_policy.schema is True
        assert other_schema.include_policy.identifier is True
        assert other_schema.include_policy.database is True
        assert other_schema.quote_policy.schema is True
        assert other_schema.quote_policy.identifier is False
        assert other_schema.quote_policy.database is False


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

    @patch(
        'dbt.adapters.bigquery.connections._is_retryable', return_value=True)
    def test_retry_and_handle(self, is_retryable):
        self.connections.DEFAULT_MAXIMUM_DELAY = 2.0

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

    @patch(
        'dbt.adapters.bigquery.connections._is_retryable', return_value=True)
    def test_retry_connection_reset(self, is_retryable):
        self.connections.open = MagicMock()
        self.connections.close = MagicMock()
        self.connections.DEFAULT_MAXIMUM_DELAY = 2.0

        @contextmanager
        def dummy_handler(msg):
            yield

        self.connections.exception_handler = dummy_handler

        def raiseConnectionResetError():
            raise ConnectionResetError("Connection broke")

        mock_conn = Mock(credentials=Mock(retries=1))
        with self.assertRaises(ConnectionResetError):
            self.connections._retry_and_handle(
              "some sql", mock_conn,
              raiseConnectionResetError)
        self.connections.close.assert_called_once_with(mock_conn)
        self.connections.open.assert_called_once_with(mock_conn)

    def test_is_retryable(self):
        _is_retryable = dbt.adapters.bigquery.connections._is_retryable
        exceptions = dbt.adapters.bigquery.impl.google.cloud.exceptions
        internal_server_error = exceptions.InternalServerError('code broke')
        bad_request_error = exceptions.BadRequest('code broke')
        connection_error = ConnectionError('code broke')
        client_error = exceptions.ClientError('bad code')
        rate_limit_error = exceptions.Forbidden("code broke", errors=[{"reason": "rateLimitExceeded"}])

        self.assertTrue(_is_retryable(internal_server_error))
        self.assertTrue(_is_retryable(bad_request_error))
        self.assertTrue(_is_retryable(connection_error))
        self.assertFalse(_is_retryable(client_error))
        self.assertTrue(_is_retryable(rate_limit_error))

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


    def test_copy_bq_table_appends(self):
        self._copy_table(
            write_disposition=dbt.adapters.bigquery.impl.WRITE_APPEND)
        args, kwargs = self.mock_client.copy_table.call_args
        self.mock_client.copy_table.assert_called_once_with(
            self._table_ref('project', 'dataset', 'table1', None),
            self._table_ref('project', 'dataset', 'table2', None),
            job_config=ANY)
        args, kwargs = self.mock_client.copy_table.call_args
        self.assertEqual(
            kwargs['job_config'].write_disposition,
            dbt.adapters.bigquery.impl.WRITE_APPEND)

    def test_copy_bq_table_truncates(self):
        self._copy_table(
            write_disposition=dbt.adapters.bigquery.impl.WRITE_TRUNCATE)
        args, kwargs = self.mock_client.copy_table.call_args
        self.mock_client.copy_table.assert_called_once_with(
            self._table_ref('project', 'dataset', 'table1', None),
            self._table_ref('project', 'dataset', 'table2', None),
            job_config=ANY)
        args, kwargs = self.mock_client.copy_table.call_args
        self.assertEqual(
            kwargs['job_config'].write_disposition,
            dbt.adapters.bigquery.impl.WRITE_TRUNCATE)

    def _table_ref(self, proj, ds, table, conn):
        return google.cloud.bigquery.table.TableReference.from_string(
            '{}.{}.{}'.format(proj, ds, table))

    def _copy_table(self, write_disposition):

        self.connections.table_ref = self._table_ref
        source = BigQueryRelation.create(
            database='project', schema='dataset', identifier='table1')
        destination = BigQueryRelation.create(
            database='project', schema='dataset', identifier='table2')
        self.connections.copy_bq_table(source, destination, write_disposition)


class TestBigQueryAdapter(BaseTestBigQueryAdapter):

    def test_copy_table_materialization_table(self):
        adapter = self.get_adapter('oauth')
        adapter.connections = MagicMock()
        adapter.copy_table('source', 'destination', 'table')
        adapter.connections.copy_bq_table.assert_called_once_with(
            'source', 'destination',
            dbt.adapters.bigquery.impl.WRITE_TRUNCATE)

    def test_copy_table_materialization_incremental(self):
        adapter = self.get_adapter('oauth')
        adapter.connections = MagicMock()
        adapter.copy_table('source', 'destination', 'incremental')
        adapter.connections.copy_bq_table.assert_called_once_with(
            'source', 'destination',
            dbt.adapters.bigquery.impl.WRITE_APPEND)

    def test_parse_partition_by(self):
        adapter = self.get_adapter('oauth')

        with self.assertRaises(dbt.exceptions.CompilationException):
            adapter.parse_partition_by("date(ts)")

        with self.assertRaises(dbt.exceptions.CompilationException):
            adapter.parse_partition_by("ts")

        self.assertEqual(
            adapter.parse_partition_by({
                "field": "ts",
            }).to_dict(omit_none=True), {
                "field": "ts",
                "data_type": "date",
                "granularity": "day"
            }
        )

        self.assertEqual(
            adapter.parse_partition_by({
                "field": "ts",
                "data_type": "date",
            }).to_dict(omit_none=True), {
                "field": "ts",
                "data_type": "date",
                "granularity": "day"
            }
        )

        self.assertEqual(
            adapter.parse_partition_by({
                "field": "ts",
                "data_type": "date",
                "granularity": "MONTH"

            }).to_dict(omit_none=True), {
                "field": "ts",
                "data_type": "date",
                "granularity": "MONTH"
            }
        )
        
        self.assertEqual(
            adapter.parse_partition_by({
                "field": "ts",
                "data_type": "date",
                "granularity": "YEAR"

            }).to_dict(omit_none=True), {
                "field": "ts",
                "data_type": "date",
                "granularity": "YEAR"
            }
        )

        self.assertEqual(
            adapter.parse_partition_by({
                "field": "ts",
                "data_type": "timestamp",
                "granularity": "HOUR"

            }).to_dict(omit_none=True), {
                "field": "ts",
                "data_type": "timestamp",
                "granularity": "HOUR"
            }
        )

        self.assertEqual(
            adapter.parse_partition_by({
                "field": "ts",
                "data_type": "timestamp",
                "granularity": "MONTH"

            }).to_dict(omit_none=True
                ), {
                "field": "ts",
                "data_type": "timestamp",
                "granularity": "MONTH"
            }
        )

        self.assertEqual(
            adapter.parse_partition_by({
                "field": "ts",
                "data_type": "timestamp",
                "granularity": "YEAR"

            }).to_dict(omit_none=True), {
                "field": "ts",
                "data_type": "timestamp",
                "granularity": "YEAR"
            }
        )

        self.assertEqual(
            adapter.parse_partition_by({
                "field": "ts",
                "data_type": "datetime",
                "granularity": "HOUR"

            }).to_dict(omit_none=True), {
                "field": "ts",
                "data_type": "datetime",
                "granularity": "HOUR"
            }
        )

        self.assertEqual(
            adapter.parse_partition_by({
                "field": "ts",
                "data_type": "datetime",
                "granularity": "MONTH"

            }).to_dict(omit_none=True), {
                "field": "ts",
                "data_type": "datetime",
                "granularity": "MONTH"
            }
        )

        self.assertEqual(
            adapter.parse_partition_by({
                "field": "ts",
                "data_type": "datetime",
                "granularity": "YEAR"

            }).to_dict(omit_none=True), {
                "field": "ts",
                "data_type": "datetime",
                "granularity": "YEAR"
            }
        )

        # Invalid, should raise an error
        with self.assertRaises(dbt.exceptions.CompilationException):
            adapter.parse_partition_by({})

        # passthrough
        self.assertEqual(
            adapter.parse_partition_by({
                "field": "id",
                "data_type": "int64",
                "range": {
                    "start": 1,
                    "end": 100,
                    "interval": 20
                }
            }).to_dict(omit_none=True
                ), {
                "field": "id",
                "data_type": "int64",
                "granularity": "day",
                "range": {
                    "start": 1,
                    "end": 100,
                    "interval": 20
                }
            }
        )

    def test_hours_to_expiration(self):
        adapter = self.get_adapter('oauth')
        mock_config = create_autospec(
            RuntimeConfigObject)
        config = {'hours_to_expiration': 4}
        mock_config.get.side_effect = lambda name: config.get(name)

        expected = {
            'expiration_timestamp': 'TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 4 hour)',
        }
        actual = adapter.get_table_options(mock_config, node={}, temporary=False)
        self.assertEqual(expected, actual)


    def test_hours_to_expiration_temporary(self):
        adapter = self.get_adapter('oauth')
        mock_config = create_autospec(
            RuntimeConfigObject)
        config={'hours_to_expiration': 4}
        mock_config.get.side_effect = lambda name: config.get(name)

        expected = {
            'expiration_timestamp': (
                'TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)'),
        }
        actual = adapter.get_table_options(mock_config, node={}, temporary=True)
        self.assertEqual(expected, actual)



class TestBigQueryFilterCatalog(unittest.TestCase):
    def test__catalog_filter_table(self):
        manifest = MagicMock()
        manifest.get_used_schemas.return_value = [['a', 'B'], ['a', '1234']]
        column_names = ['table_name', 'table_database', 'table_schema', 'something']
        rows = [
            ['foo', 'a', 'b', '1234'],  # include
            ['foo', 'a', '1234', '1234'],  # include, w/ table schema as str
            ['foo', 'c', 'B', '1234'],  # skip
            ['1234', 'A', 'B', '1234'],  # include, w/ table name as str
        ]
        table = agate.Table(
            rows, column_names, agate_helper.DEFAULT_TYPE_TESTER
        )

        result = BigQueryAdapter._catalog_filter_table(table, manifest)
        assert len(result) == 3
        for row in result.rows:
            assert isinstance(row['table_schema'], str)
            assert isinstance(row['table_database'], str)
            assert isinstance(row['table_name'], str)
            assert isinstance(row['something'], decimal.Decimal)


class TestBigQueryAdapterConversions(TestAdapterConversions):
    def test_convert_text_type(self):
        rows = [
            ['', 'a1', 'stringval1'],
            ['', 'a2', 'stringvalasdfasdfasdfa'],
            ['', 'a3', 'stringval3'],
        ]
        agate_table = self._make_table_of(rows, agate.Text)
        expected = ['string', 'string', 'string']
        for col_idx, expect in enumerate(expected):
            assert BigQueryAdapter.convert_text_type(agate_table, col_idx) == expect

    def test_convert_number_type(self):
        rows = [
            ['', '23.98', '-1'],
            ['', '12.78', '-2'],
            ['', '79.41', '-3'],
        ]
        agate_table = self._make_table_of(rows, agate.Number)
        expected = ['int64', 'float64', 'int64']
        for col_idx, expect in enumerate(expected):
            assert BigQueryAdapter.convert_number_type(agate_table, col_idx) == expect

    def test_convert_boolean_type(self):
        rows = [
            ['', 'false', 'true'],
            ['', 'false', 'false'],
            ['', 'false', 'true'],
        ]
        agate_table = self._make_table_of(rows, agate.Boolean)
        expected = ['bool', 'bool', 'bool']
        for col_idx, expect in enumerate(expected):
            assert BigQueryAdapter.convert_boolean_type(agate_table, col_idx) == expect

    def test_convert_datetime_type(self):
        rows = [
            ['', '20190101T01:01:01Z', '2019-01-01 01:01:01'],
            ['', '20190102T01:01:01Z', '2019-01-01 01:01:01'],
            ['', '20190103T01:01:01Z', '2019-01-01 01:01:01'],
        ]
        agate_table = self._make_table_of(rows, [agate.DateTime, agate_helper.ISODateTime, agate.DateTime])
        expected = ['datetime', 'datetime', 'datetime']
        for col_idx, expect in enumerate(expected):
            assert BigQueryAdapter.convert_datetime_type(agate_table, col_idx) == expect

    def test_convert_date_type(self):
        rows = [
            ['', '2019-01-01', '2019-01-04'],
            ['', '2019-01-02', '2019-01-04'],
            ['', '2019-01-03', '2019-01-04'],
        ]
        agate_table = self._make_table_of(rows, agate.Date)
        expected = ['date', 'date', 'date']
        for col_idx, expect in enumerate(expected):
            assert BigQueryAdapter.convert_date_type(agate_table, col_idx) == expect

    def test_convert_time_type(self):
        # dbt's default type testers actually don't have a TimeDelta at all.
        agate.TimeDelta
        rows = [
            ['', '120s', '10s'],
            ['', '3m', '11s'],
            ['', '1h', '12s'],
        ]
        agate_table = self._make_table_of(rows, agate.TimeDelta)
        expected = ['time', 'time', 'time']
        for col_idx, expect in enumerate(expected):
            assert BigQueryAdapter.convert_time_type(agate_table, col_idx) == expect
