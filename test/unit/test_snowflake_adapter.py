import mock
import unittest

import dbt.flags as flags

import dbt.adapters
from dbt.adapters.snowflake import SnowflakeAdapter
from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from snowflake import connector as snowflake_connector

class TestSnowflakeAdapter(unittest.TestCase):
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

        self.handle = mock.MagicMock(spec=snowflake_connector.SnowflakeConnection)
        self.cursor = self.handle.cursor.return_value
        self.mock_execute = self.cursor.execute
        self.patcher = mock.patch('dbt.adapters.snowflake.impl.snowflake.connector.connect')
        self.snowflake = self.patcher.start()

        self.snowflake.return_value = self.handle
        conn = SnowflakeAdapter.get_connection(self.profile)

    def tearDown(self):
        # we want a unique self.handle every time.
        SnowflakeAdapter.cleanup_connections()
        self.patcher.stop()

    def test_quoting_on_drop_schema(self):
        SnowflakeAdapter.drop_schema(
            profile=self.profile,
            project_cfg=self.project,
            schema='test_schema'
        )

        self.mock_execute.assert_has_calls([
            mock.call('drop schema if exists "test_schema" cascade', None)
        ])

    def test_quoting_on_drop(self):
        SnowflakeAdapter.drop(
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
        SnowflakeAdapter.truncate(
            profile=self.profile,
            project_cfg=self.project,
            schema='test_schema',
            table='test_table'
        )
        self.mock_execute.assert_has_calls([
            mock.call('truncate table "test_schema".test_table', None)
        ])

    def test_quoting_on_rename(self):
        SnowflakeAdapter.rename(
            profile=self.profile,
            project_cfg=self.project,
            schema='test_schema',
            from_name='table_a',
            to_name='table_b'
        )
        self.mock_execute.assert_has_calls([
            mock.call('alter table "test_schema".table_a rename to table_b', None)
        ])
