import mock
import unittest

import dbt.flags as flags

import dbt.adapters
from dbt.adapters.postgres import PostgresAdapter
from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from psycopg2 import extensions as psycopg2_extensions
import agate

from .utils import config_from_parts_or_dicts


class TestPostgresAdapter(unittest.TestCase):

    def setUp(self):
        flags.STRICT_MODE = True
        project_cfg = {
            'name': 'X',
            'version': '0.1',
            'profile': 'test',
            'project-root': '/tmp/dbt/does-not-exist',
        }
        profile_cfg = {
            'outputs': {
                'test': {
                    'type': 'postgres',
                    'dbname': 'postgres',
                    'user': 'root',
                    'host': 'database',
                    'pass': 'password',
                    'port': 5432,
                    'schema': 'public'
                }
            },
            'target': 'test'
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)

    @property
    def adapter(self):
        return PostgresAdapter(self.config)

    def test_acquire_connection_validations(self):
        try:
            connection = self.adapter.acquire_connection('dummy')
            self.assertEquals(connection.type, 'postgres')
        except ValidationException as e:
            self.fail('got ValidationException: {}'.format(str(e)))
        except BaseException as e:
            self.fail('validation failed with unknown exception: {}'
                      .format(str(e)))

    def test_acquire_connection(self):
        connection = self.adapter.acquire_connection('dummy')

        self.assertEquals(connection.state, 'open')
        self.assertNotEquals(connection.handle, None)

    @mock.patch('dbt.adapters.postgres.impl.psycopg2')
    def test_default_keepalive(self, psycopg2):
        connection = self.adapter.acquire_connection('dummy')

        psycopg2.connect.assert_called_once_with(
            dbname='postgres',
            user='root',
            host='database',
            password='password',
            port=5432,
            connect_timeout=10)

    @mock.patch('dbt.adapters.postgres.impl.psycopg2')
    def test_changed_keepalive(self, psycopg2):
        credentials = self.adapter.config.credentials.incorporate(
            keepalives_idle=256
        )
        self.adapter.config.credentials = credentials
        connection = self.adapter.acquire_connection('dummy')

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
        credentials = self.config.credentials.incorporate(keepalives_idle=0)
        self.config.credentials = credentials
        connection = self.adapter.acquire_connection('dummy')

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

        mock_manifest = mock.MagicMock()
        mock_manifest.get_used_schemas.return_value = {'foo', 'quux'}

        catalog = self.adapter.get_catalog(mock_manifest)
        self.assertEqual(
            set(map(tuple, catalog)),
            {('foo', 'bar'), ('FOO', 'baz'), ('quux', 'bar')}
        )


class TestConnectingPostgresAdapter(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = False

        profile_cfg = {
            'outputs': {
                'test': {
                    'type': 'postgres',
                    'dbname': 'postgres',
                    'user': 'root',
                    'host': 'database',
                    'pass': 'password',
                    'port': 5432,
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

        self.handle = mock.MagicMock(spec=psycopg2_extensions.connection)
        self.cursor = self.handle.cursor.return_value
        self.mock_execute = self.cursor.execute
        self.patcher = mock.patch('dbt.adapters.postgres.impl.psycopg2')
        self.psycopg2 = self.patcher.start()

        self.psycopg2.connect.return_value = self.handle
        self.adapter = PostgresAdapter(self.config)
        self.adapter.get_connection()

    def tearDown(self):
        # we want a unique self.handle every time.
        self.adapter.cleanup_connections()
        self.patcher.stop()

    def test_quoting_on_drop_schema(self):
        self.adapter.drop_schema(schema='test_schema')

        self.mock_execute.assert_has_calls([
            mock.call('drop schema if exists "test_schema" cascade', None)
        ])

    def test_quoting_on_drop(self):
        self.adapter.drop(
            schema='test_schema',
            relation='test_table',
            relation_type='table'
        )
        self.mock_execute.assert_has_calls([
            mock.call('drop table if exists "test_schema".test_table cascade', None)
        ])

    def test_quoting_on_truncate(self):
        self.adapter.truncate(
            schema='test_schema',
            table='test_table'
        )
        self.mock_execute.assert_has_calls([
            mock.call('truncate table "test_schema".test_table', None)
        ])

    def test_quoting_on_rename(self):
        self.adapter.rename(
            schema='test_schema',
            from_name='table_a',
            to_name='table_b'
        )
        self.mock_execute.assert_has_calls([
            mock.call('alter table "test_schema".table_a rename to table_b', None)
        ])
