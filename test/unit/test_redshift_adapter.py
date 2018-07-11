import unittest
import mock

import dbt.adapters
import dbt.flags as flags
import dbt.utils

from dbt.adapters.redshift import RedshiftAdapter
from dbt.exceptions import ValidationException, FailedToConnectException
from dbt.logger import GLOBAL_LOGGER as logger  # noqa

@classmethod
def fetch_cluster_credentials(*args, **kwargs):
    return {
        'DbUser': 'root',
        'DbPassword': 'tmp_password'
    }

class TestRedshiftAdapter(unittest.TestCase):

    def setUp(self):
        flags.STRICT_MODE = True

        self.profile = {
            'dbname': 'redshift',
            'user': 'root',
            'host': 'database',
            'pass': 'password',
            'port': 5439,
            'schema': 'public'
        }

    def test_implicit_database_conn(self):
        creds = RedshiftAdapter.get_credentials(self.profile)
        self.assertEquals(creds, self.profile)

    def test_explicit_database_conn(self):
        self.profile['method'] = 'database'

        creds = RedshiftAdapter.get_credentials(self.profile)
        self.assertEquals(creds, self.profile)

    def test_explicit_iam_conn(self):
        self.profile.update({
            'method': 'iam',
            'cluster_id': 'my_redshift',
            'iam_duration_s': 1200,
        })

        with mock.patch.object(RedshiftAdapter, 'fetch_cluster_credentials', new=fetch_cluster_credentials):
            creds = RedshiftAdapter.get_credentials(self.profile)

        expected_creds = dbt.utils.merge(self.profile, {'pass': 'tmp_password'})
        self.assertEquals(creds, expected_creds)

    def test_invalid_auth_method(self):
        self.profile['method'] = 'badmethod'

        with self.assertRaises(dbt.exceptions.FailedToConnectException) as context:
            with mock.patch.object(RedshiftAdapter, 'fetch_cluster_credentials', new=fetch_cluster_credentials):
                RedshiftAdapter.get_credentials(self.profile)

        self.assertTrue('badmethod' in context.exception.msg)

    def test_invalid_iam_no_cluster_id(self):
        self.profile['method'] = 'iam'
        with self.assertRaises(dbt.exceptions.FailedToConnectException) as context:
            with mock.patch.object(RedshiftAdapter, 'fetch_cluster_credentials', new=fetch_cluster_credentials):
                RedshiftAdapter.get_credentials(self.profile)

        self.assertTrue("'cluster_id' must be provided" in context.exception.msg)


    @mock.patch('dbt.adapters.postgres.impl.psycopg2')
    def test_default_keepalive(self, psycopg2):
        connection = RedshiftAdapter.acquire_connection(self.profile, 'dummy')

        psycopg2.connect.assert_called_once_with(
            dbname='redshift',
            user='root',
            host='database',
            password='password',
            port=5439,
            connect_timeout=10,
            keepalives_idle=RedshiftAdapter.DEFAULT_TCP_KEEPALIVE)

    @mock.patch('dbt.adapters.postgres.impl.psycopg2')
    def test_changed_keepalive(self, psycopg2):
        self.profile['keepalives_idle'] = 256
        connection = RedshiftAdapter.acquire_connection(self.profile, 'dummy')

        psycopg2.connect.assert_called_once_with(
            dbname='redshift',
            user='root',
            host='database',
            password='password',
            port=5439,
            connect_timeout=10,
            keepalives_idle=256)

    @mock.patch('dbt.adapters.postgres.impl.psycopg2')
    def test_set_zero_keepalive(self, psycopg2):
        self.profile['keepalives_idle'] = 0
        connection = RedshiftAdapter.acquire_connection(self.profile, 'dummy')

        psycopg2.connect.assert_called_once_with(
            dbname='redshift',
            user='root',
            host='database',
            password='password',
            port=5439,
            connect_timeout=10)
