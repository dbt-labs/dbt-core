import mock
import unittest

import dbt.flags as flags

import dbt.adapters
from dbt.adapters.postgres import PostgresAdapter
from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger  # noqa


class TestPostgresAdapter(unittest.TestCase):

    def setUp(self):
        flags.STRICT_MODE = True

        self.profile = {
            'dbname': 'postgres',
            'user': 'root',
            'host': 'database',
            'pass': 'password',
            'port': 5432,
            'schema': 'public'
        }

    def test_acquire_connection_validations(self):
        try:
            connection = PostgresAdapter.acquire_connection(self.profile,
                                                            'dummy')
            self.assertEquals(connection.get('type'), 'postgres')
        except ValidationException as e:
            self.fail('got ValidationException: {}'.format(str(e)))
        except BaseException as e:
            self.fail('validation failed with unknown exception: {}'
                      .format(str(e)))

    def test_acquire_connection(self):
        connection = PostgresAdapter.acquire_connection(self.profile, 'dummy')

        self.assertEquals(connection.get('state'), 'open')
        self.assertNotEquals(connection.get('handle'), None)

    @mock.patch('dbt.adapters.postgres.impl.psycopg2')
    def test_default_keepalive(self, psycopg2):
        connection = PostgresAdapter.acquire_connection(self.profile, 'dummy')

        psycopg2.connect.assert_called_once_with(
            dbname='postgres',
            user='root',
            host='database',
            password='password',
            port=5432,
            connect_timeout=10)

    @mock.patch('dbt.adapters.postgres.impl.psycopg2')
    def test_changed_keepalive(self, psycopg2):
        self.profile['keepalives_idle'] = 256
        connection = PostgresAdapter.acquire_connection(self.profile, 'dummy')

        psycopg2.connect.assert_called_once_with(
            dbname='postgres',
            user='root',
            host='database',
            password='password',
            port=5432,
            connect_timeout=10,
            keepalives_idle=256)

    @mock.patch('dbt.adapters.postgres.impl.psycopg2')
    def test_set_zero_keepalive(self, psycopg2):
        self.profile['keepalives_idle'] = 0
        connection = PostgresAdapter.acquire_connection(self.profile, 'dummy')

        psycopg2.connect.assert_called_once_with(
            dbname='postgres',
            user='root',
            host='database',
            password='password',
            port=5432,
            connect_timeout=10)

