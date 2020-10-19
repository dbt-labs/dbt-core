import unittest
from unittest import mock
from unittest.mock import Mock

import agate
import boto3

import dbt.adapters  # noqa
import dbt.flags as flags

from dbt.adapters.redshift import (
    RedshiftAdapter,
    Plugin as RedshiftPlugin,
)
from dbt.clients import agate_helper
from dbt.exceptions import FailedToConnectException
from dbt.logger import GLOBAL_LOGGER as logger  # noqa

from .utils import config_from_parts_or_dicts, mock_connection, TestAdapterConversions, inject_adapter


@classmethod
def fetch_cluster_credentials(*args, **kwargs):
    return {
        'DbUser': 'root',
        'DbPassword': 'tmp_password'
    }


class TestRedshiftAdapter(unittest.TestCase):

    def setUp(self):
        flags.STRICT_MODE = True

        profile_cfg = {
            'outputs': {
                'test': {
                    'type': 'redshift',
                    'dbname': 'redshift',
                    'user': 'root',
                    'host': 'thishostshouldnotexist',
                    'pass': 'password',
                    'port': 5439,
                    'schema': 'public'
                }
            },
            'target': 'test'
        }

        project_cfg = {
            'name': 'X',
            'version': '0.1',
            'profile': 'test',
            'project-root': '/tmp/dbt/does-not-exist',
            'quoting': {
                'identifier': False,
                'schema': True,
            },
            'config-version': 2,
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self._adapter = None

    @property
    def adapter(self):
        if self._adapter is None:
            self._adapter = RedshiftAdapter(self.config)
            inject_adapter(self._adapter, RedshiftPlugin)
        return self._adapter

    def test_implicit_database_conn(self):
        creds = RedshiftAdapter.ConnectionManager.get_credentials(self.config.credentials)
        self.assertEqual(creds, self.config.credentials)

    def test_explicit_database_conn(self):
        self.config.method = 'database'

        creds = RedshiftAdapter.ConnectionManager.get_credentials(self.config.credentials)
        self.assertEqual(creds, self.config.credentials)

    def test_explicit_iam_conn(self):
        self.config.credentials = self.config.credentials.replace(
            method='iam',
            cluster_id='my_redshift',
            iam_duration_seconds=1200
        )

        with mock.patch.object(RedshiftAdapter.ConnectionManager, 'fetch_cluster_credentials', new=fetch_cluster_credentials):
            creds = RedshiftAdapter.ConnectionManager.get_credentials(self.config.credentials)

        expected_creds = self.config.credentials.replace(password='tmp_password')
        self.assertEqual(creds, expected_creds)

    def test_iam_conn_optionals(self):

        profile_cfg = {
            'outputs': {
                'test': {
                    'type': 'redshift',
                    'dbname': 'redshift',
                    'user': 'root',
                    'host': 'thishostshouldnotexist',
                    'port': 5439,
                    'schema': 'public',
                    'method': 'iam',
                    'cluster_id': 'my_redshift',
                    'db_groups': ["my_dbgroup"],
                    'autocreate': True,
                }
            },
            'target': 'test'
        }

        config_from_parts_or_dicts(self.config, profile_cfg)

    def test_invalid_auth_method(self):
        # we have to set method this way, otherwise it won't validate
        self.config.credentials.method = 'badmethod'

        with self.assertRaises(FailedToConnectException) as context:
            with mock.patch.object(RedshiftAdapter.ConnectionManager, 'fetch_cluster_credentials', new=fetch_cluster_credentials):
                RedshiftAdapter.ConnectionManager.get_credentials(self.config.credentials)

        self.assertTrue('badmethod' in context.exception.msg)

    def test_invalid_iam_no_cluster_id(self):
        self.config.credentials = self.config.credentials.replace(method='iam')
        with self.assertRaises(FailedToConnectException) as context:
            with mock.patch.object(RedshiftAdapter.ConnectionManager, 'fetch_cluster_credentials', new=fetch_cluster_credentials):
                RedshiftAdapter.ConnectionManager.get_credentials(self.config.credentials)

        self.assertTrue("'cluster_id' must be provided" in context.exception.msg)

    def test_default_session_is_not_used_when_iam_used(self):
        boto3.DEFAULT_SESSION = Mock()
        self.config.credentials = self.config.credentials.replace(method='iam')
        self.config.credentials.cluster_id = 'clusterid'
        with mock.patch('dbt.adapters.redshift.connections.boto3.Session'):
            RedshiftAdapter.ConnectionManager.get_credentials(self.config.credentials)
            self.assertEquals(boto3.DEFAULT_SESSION.client.call_count, 0,
                              "The redshift client should not be created using the default session because the session object is not thread-safe")

    def test_default_session_is_not_used_when_iam_not_used(self):
        boto3.DEFAULT_SESSION = Mock()
        self.config.credentials = self.config.credentials.replace(method=None)
        with mock.patch('dbt.adapters.redshift.connections.boto3.Session'):
            RedshiftAdapter.ConnectionManager.get_credentials(self.config.credentials)
            self.assertEquals(boto3.DEFAULT_SESSION.client.call_count, 0,
                              "The redshift client should not be created using the default session because the session object is not thread-safe")

    def test_cancel_open_connections_empty(self):
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_master(self):
        key = self.adapter.connections.get_thread_identifier()
        self.adapter.connections.thread_connections[key] = mock_connection('master')
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_single(self):
        master = mock_connection('master')
        model = mock_connection('model')
        model.handle.get_backend_pid.return_value = 42

        key = self.adapter.connections.get_thread_identifier()
        self.adapter.connections.thread_connections.update({
            key: master,
            1: model,
        })
        with mock.patch.object(self.adapter.connections, 'add_query') as add_query:
            query_result = mock.MagicMock()
            add_query.return_value = (None, query_result)

            self.assertEqual(len(list(self.adapter.cancel_open_connections())), 1)

            add_query.assert_called_once_with('select pg_terminate_backend(42)')

        master.handle.get_backend_pid.assert_not_called()

    @mock.patch('dbt.adapters.postgres.connections.psycopg2')
    def test_default_keepalive(self, psycopg2):
        connection = self.adapter.acquire_connection('dummy')

        psycopg2.connect.assert_not_called()
        connection.handle
        psycopg2.connect.assert_called_once_with(
            dbname='redshift',
            user='root',
            host='thishostshouldnotexist',
            password='password',
            port=5439,
            connect_timeout=10,
            keepalives_idle=240
        )

    @mock.patch('dbt.adapters.postgres.connections.psycopg2')
    def test_changed_keepalive(self, psycopg2):
        self.config.credentials = self.config.credentials.replace(keepalives_idle=256)
        connection = self.adapter.acquire_connection('dummy')

        psycopg2.connect.assert_not_called()
        connection.handle
        psycopg2.connect.assert_called_once_with(
            dbname='redshift',
            user='root',
            host='thishostshouldnotexist',
            password='password',
            port=5439,
            connect_timeout=10,
            keepalives_idle=256)

    @mock.patch('dbt.adapters.postgres.connections.psycopg2')
    def test_search_path(self, psycopg2):
        self.config.credentials = self.config.credentials.replace(search_path="test")
        connection = self.adapter.acquire_connection('dummy')

        psycopg2.connect.assert_not_called()
        connection.handle
        psycopg2.connect.assert_called_once_with(
            dbname='redshift',
            user='root',
            host='thishostshouldnotexist',
            password='password',
            port=5439,
            connect_timeout=10,
            options="-c search_path=test",
            keepalives_idle=240)

    @mock.patch('dbt.adapters.postgres.connections.psycopg2')
    def test_search_path_with_space(self, psycopg2):
        self.config.credentials = self.config.credentials.replace(search_path="test test")
        connection = self.adapter.acquire_connection('dummy')

        psycopg2.connect.assert_not_called()
        connection.handle
        psycopg2.connect.assert_called_once_with(
            dbname='redshift',
            user='root',
            host='thishostshouldnotexist',
            password='password',
            port=5439,
            connect_timeout=10,
            options=r"-c search_path=test\ test",
            keepalives_idle=240)

    @mock.patch('dbt.adapters.postgres.connections.psycopg2')
    def test_set_zero_keepalive(self, psycopg2):
        self.config.credentials = self.config.credentials.replace(keepalives_idle=0)
        connection = self.adapter.acquire_connection('dummy')

        psycopg2.connect.assert_not_called()
        connection.handle
        psycopg2.connect.assert_called_once_with(
            dbname='redshift',
            user='root',
            host='thishostshouldnotexist',
            password='password',
            port=5439,
            connect_timeout=10)

    def test_dbname_verification_is_case_insensitive(self):
        # Override adapter settings from setUp()
        profile_cfg = {
            'outputs': {
                'test': {
                    'type': 'redshift',
                    'dbname': 'Redshift',
                    'user': 'root',
                    'host': 'thishostshouldnotexist',
                    'pass': 'password',
                    'port': 5439,
                    'schema': 'public'
                }
            },
            'target': 'test'
        }

        project_cfg = {
            'name': 'X',
            'version': '0.1',
            'profile': 'test',
            'project-root': '/tmp/dbt/does-not-exist',
            'quoting': {
                'identifier': False,
                'schema': True,
            },
            'config-version': 2,
        }
        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self.adapter.cleanup_connections()
        self._adapter = RedshiftAdapter(self.config)
        self.adapter.verify_database('redshift')


class TestRedshiftAdapterConversions(TestAdapterConversions):
    def test_convert_text_type(self):
        rows = [
            ['', 'a1', 'stringval1'],
            ['', 'a2', 'stringvalasdfasdfasdfa'],
            ['', 'a3', 'stringval3'],
        ]
        agate_table = self._make_table_of(rows, agate.Text)
        expected = ['varchar(64)', 'varchar(2)', 'varchar(22)']
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_text_type(agate_table, col_idx) == expect

    def test_convert_number_type(self):
        rows = [
            ['', '23.98', '-1'],
            ['', '12.78', '-2'],
            ['', '79.41', '-3'],
        ]
        agate_table = self._make_table_of(rows, agate.Number)
        expected = ['integer', 'float8', 'integer']
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_number_type(agate_table, col_idx) == expect

    def test_convert_boolean_type(self):
        rows = [
            ['', 'false', 'true'],
            ['', 'false', 'false'],
            ['', 'false', 'true'],
        ]
        agate_table = self._make_table_of(rows, agate.Boolean)
        expected = ['boolean', 'boolean', 'boolean']
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_boolean_type(agate_table, col_idx) == expect

    def test_convert_datetime_type(self):
        rows = [
            ['', '20190101T01:01:01Z', '2019-01-01 01:01:01'],
            ['', '20190102T01:01:01Z', '2019-01-01 01:01:01'],
            ['', '20190103T01:01:01Z', '2019-01-01 01:01:01'],
        ]
        agate_table = self._make_table_of(rows, [agate.DateTime, agate_helper.ISODateTime, agate.DateTime])
        expected = ['timestamp without time zone', 'timestamp without time zone', 'timestamp without time zone']
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_datetime_type(agate_table, col_idx) == expect

    def test_convert_date_type(self):
        rows = [
            ['', '2019-01-01', '2019-01-04'],
            ['', '2019-01-02', '2019-01-04'],
            ['', '2019-01-03', '2019-01-04'],
        ]
        agate_table = self._make_table_of(rows, agate.Date)
        expected = ['date', 'date', 'date']
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_date_type(agate_table, col_idx) == expect

    def test_convert_time_type(self):
        # dbt's default type testers actually don't have a TimeDelta at all.
        agate.TimeDelta
        rows = [
            ['', '120s', '10s'],
            ['', '3m', '11s'],
            ['', '1h', '12s'],
        ]
        agate_table = self._make_table_of(rows, agate.TimeDelta)
        expected = ['varchar(24)', 'varchar(24)', 'varchar(24)']
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_time_type(agate_table, col_idx) == expect


# convert_boolean_type
# convert_datetime_type
# convert_date_type
# convert_time_type
