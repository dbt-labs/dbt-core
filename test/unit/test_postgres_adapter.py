import mock
import unittest

import dbt.flags as flags

import dbt.adapters
from dbt.adapters.postgres import PostgresAdapter
from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from psycopg2 import extensions as psycopg2_extensions
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

class TestConnectingPostgresAdapter(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = False

        self.profile = {
            'dbname': 'postgres',
            'user': 'root',
            'host': 'database',
            'pass': 'password',
            'port': 5432,
            'schema': 'public'
        }

        self.project = {
            'name': 'X',
            'version': '0.1',
            'profile': 'test',
            'project-root': '/tmp/dbt/does-not-exist',
            'quoting': {
                'identifier': False,
                'schema': True,
            }
        }

        self.handle = mock.MagicMock(spec=psycopg2_extensions.connection)
        self.cursor = self.handle.cursor.return_value
        self.mock_execute = self.cursor.execute
        self.patcher = mock.patch('dbt.adapters.postgres.impl.psycopg2')
        self.psycopg2 = self.patcher.start()

        self.psycopg2.connect.return_value = self.handle
        conn = PostgresAdapter.get_connection(self.profile)

    def tearDown(self):
        # we want a unique self.handle every time.
        PostgresAdapter.cleanup_connections()
        self.patcher.stop()

    def test_quoting_on_drop_schema(self):
        PostgresAdapter.drop_schema(
            profile=self.profile,
            project_cfg=self.project,
            schema='test_schema'
        )

        self.mock_execute.assert_has_calls([
            mock.call('drop schema if exists "test_schema" cascade', None)
        ])

    def test_quoting_on_drop(self):
        PostgresAdapter.drop(
            profile=self.profile,
            project_cfg=self.project,
            schema='test_schema',
            relation='test_table',
            relation_type='table'
        )
        self.mock_execute.assert_has_calls([
            mock.call('drop table if exists "test_schema".test_table cascade', None)
        ])

    def test_quoting_on_truncate(self):
        PostgresAdapter.truncate(
            profile=self.profile,
            project_cfg=self.project,
            schema='test_schema',
            table='test_table'
        )
        self.mock_execute.assert_has_calls([
            mock.call('truncate table "test_schema".test_table', None)
        ])

    def test_quoting_on_rename(self):
        PostgresAdapter.rename(
            profile=self.profile,
            project_cfg=self.project,
            schema='test_schema',
            from_name='table_a',
            to_name='table_b'
        )
        self.mock_execute.assert_has_calls([
            mock.call('alter table "test_schema".table_a rename to table_b', None)
        ])
