import mock
import unittest

import dbt.flags as flags

import dbt.adapters
from dbt.adapters.snowflake import SnowflakeAdapter
from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from snowflake import connector as snowflake_connector

from .utils import config_from_parts_or_dicts

class TestSnowflakeAdapter(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = False

        profile_cfg = {
            'outputs': {
                'test': {
                    'type': 'snowflake',
                    'account': 'test_account',
                    'user': 'test_user',
                    'password': 'test_password',
                    'database': 'test_databse',
                    'warehouse': 'test_warehouse',
                    'schema': 'public',
                },
            },
            'target': 'test',
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

        self.handle = mock.MagicMock(spec=snowflake_connector.SnowflakeConnection)
        self.cursor = self.handle.cursor.return_value
        self.mock_execute = self.cursor.execute
        self.patcher = mock.patch('dbt.adapters.snowflake.impl.snowflake.connector.connect')
        self.snowflake = self.patcher.start()

        self.snowflake.return_value = self.handle
        self.adapter = SnowflakeAdapter(self.config)
        self.adapter.get_connection()

    def tearDown(self):
        # we want a unique self.handle every time.
        self.adapter.cleanup_connections()
        self.patcher.stop()

    def test_quoting_on_drop_schema(self):
        self.adapter.drop_schema(
            schema='test_schema'
        )

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

    def test_client_session_keep_alive_false_by_default(self):
        self.snowflake.assert_has_calls([
            mock.call(
                account='test_account', autocommit=False,
                client_session_keep_alive=False, database='test_databse',
                password='test_password', role=None, schema='public',
                user='test_user', warehouse='test_warehouse')
        ])

    def test_client_session_keep_alive_true(self):
        self.config.credentials = self.config.credentials.incorporate(
            client_session_keep_alive=True)
        self.adapter = SnowflakeAdapter(self.config)
        self.adapter.get_connection(name='new_connection_with_new_config')

        self.snowflake.assert_has_calls([
            mock.call(
                account='test_account', autocommit=False,
                client_session_keep_alive=True, database='test_databse',
                password='test_password', role=None, schema='public',
                user='test_user', warehouse='test_warehouse')
        ])
