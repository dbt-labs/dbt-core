import mock
import unittest

import dbt.flags as flags

import dbt.adapters
from dbt.adapters.postgres import PostgresAdapter
from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
import agate


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

    @mock.patch.object(PostgresAdapter, 'run_operation')
    def test_get_catalog_various_schemas(self, mock_run):
        column_names = ['table_schema', 'table_name']
        rows = [
            ('foo', 'bar'),
            ('FOO', 'baz'),
            (None, 'bar'),
            ('quux', 'bar'),
            ('skip', 'bar')
        ]
        mock_run.return_value = agate.Table(rows=rows,
                                            column_names=column_names)

        # we should accept the lowercase matching 'foo's only.
        mock_nodes = [
            mock.MagicMock(spec_set=['schema'], schema='foo')
            for k in range(2)
        ]
        mock_nodes.append(mock.MagicMock(spec_set=['schema'], schema='quux'))
        nodes = {str(idx): n for idx, n in enumerate(mock_nodes)}
        # give manifest the dict it wants
        mock_manifest = mock.MagicMock(spec_set=['nodes'], nodes=nodes)

        catalog = PostgresAdapter.get_catalog({}, {}, mock_manifest)
        self.assertEqual(
            set(map(tuple, catalog)),
            {('foo', 'bar'), ('FOO', 'baz'), ('quux', 'bar')}
        )
