import unittest
import mock

import dbt.adapters
import dbt.flags as flags
import dbt.utils

from dbt.adapters.redshift import RedshiftAdapter
from dbt.exceptions import ValidationException, FailedToConnectException
from dbt.logger import GLOBAL_LOGGER as logger  # noqa

from .utils import config_from_parts_or_dicts


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
                    'host': 'database',
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
            }
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self._adapter = None

    @property
    def adapter(self):
        if self._adapter is None:
            self._adapter = RedshiftAdapter(self.config)
        return self._adapter

    def test_implicit_database_conn(self):
        creds = RedshiftAdapter.ConnectionManager.get_credentials(self.config.credentials)
        self.assertEquals(creds, self.config.credentials)

    def test_explicit_database_conn(self):
        self.config.method = 'database'

        creds = RedshiftAdapter.ConnectionManager.get_credentials(self.config.credentials)
        self.assertEquals(creds, self.config.credentials)

    def test_explicit_iam_conn(self):
        self.config.credentials = self.config.credentials.incorporate(
            method='iam',
            cluster_id='my_redshift',
            iam_duration_seconds=1200
        )

        with mock.patch.object(RedshiftAdapter.ConnectionManager, 'fetch_cluster_credentials', new=fetch_cluster_credentials):
            creds = RedshiftAdapter.ConnectionManager.get_credentials(self.config.credentials)

        expected_creds = self.config.credentials.incorporate(password='tmp_password')
        self.assertEquals(creds, expected_creds)

    def test_invalid_auth_method(self):
        # we have to set method this way, otherwise it won't validate
        self.config.credentials._contents['method'] = 'badmethod'

        with self.assertRaises(dbt.exceptions.FailedToConnectException) as context:
            with mock.patch.object(RedshiftAdapter.ConnectionManager, 'fetch_cluster_credentials', new=fetch_cluster_credentials):
                RedshiftAdapter.ConnectionManager.get_credentials(self.config.credentials)

        self.assertTrue('badmethod' in context.exception.msg)

    def test_invalid_iam_no_cluster_id(self):
        self.config.credentials = self.config.credentials.incorporate(
            method='iam'
        )
        with self.assertRaises(dbt.exceptions.FailedToConnectException) as context:
            with mock.patch.object(RedshiftAdapter.ConnectionManager, 'fetch_cluster_credentials', new=fetch_cluster_credentials):
                RedshiftAdapter.ConnectionManager.get_credentials(self.config.credentials)

        self.assertTrue("'cluster_id' must be provided" in context.exception.msg)

    def test_cancel_open_connections_empty(self):
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_master(self):
        self.adapter.connections.in_use['master'] = mock.MagicMock()
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_single(self):
        master = mock.MagicMock()
        model = mock.MagicMock()
        model.handle.get_backend_pid.return_value = 42

        self.adapter.connections.in_use.update({
            'master': master,
            'model': model,
        })
        with mock.patch.object(self.adapter.connections, 'add_query') as add_query:
            query_result = mock.MagicMock()
            add_query.return_value = (None, query_result)

            self.assertEqual(len(list(self.adapter.cancel_open_connections())), 1)

            add_query.assert_called_once_with('select pg_terminate_backend(42)', 'master')

        master.handle.get_backend_pid.assert_not_called()

    @mock.patch('dbt.adapters.postgres.connections.psycopg2')
    def test_default_keepalive(self, psycopg2):
        connection = self.adapter.acquire_connection('dummy')

        psycopg2.connect.assert_called_once_with(
            dbname='redshift',
            user='root',
            host='database',
            password='password',
            port=5439,
            connect_timeout=10,
            keepalives_idle=RedshiftAdapter.ConnectionManager.DEFAULT_TCP_KEEPALIVE
        )

    @mock.patch('dbt.adapters.postgres.connections.psycopg2')
    def test_changed_keepalive(self, psycopg2):
        self.config.credentials = self.config.credentials.incorporate(
            keepalives_idle=256
        )
        connection = self.adapter.acquire_connection('dummy')

        psycopg2.connect.assert_called_once_with(
            dbname='redshift',
            user='root',
            host='database',
            password='password',
            port=5439,
            connect_timeout=10,
            keepalives_idle=256)

    @mock.patch('dbt.adapters.postgres.connections.psycopg2')
    def test_set_zero_keepalive(self, psycopg2):
        self.config.credentials = self.config.credentials.incorporate(
            keepalives_idle=0
        )
        connection = self.adapter.acquire_connection('dummy')

        psycopg2.connect.assert_called_once_with(
            dbname='redshift',
            user='root',
            host='database',
            password='password',
            port=5439,
            connect_timeout=10)
