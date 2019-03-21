from mock import patch

import mock
import unittest

import dbt.flags as flags

import dbt.adapters
from dbt.adapters.snowflake import SnowflakeAdapter
from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from snowflake import connector as snowflake_connector

from .utils import config_from_parts_or_dicts, inject_adapter


class TestSnowflakeAdapter(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = False

        profile_cfg = {
            'outputs': {
                'test': {
                    'type': 'snowflake',
                    'account': 'test_account',
                    'user': 'test_user',
                    'database': 'test_database',
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

        self.handle = mock.MagicMock(
            spec=snowflake_connector.SnowflakeConnection)
        self.cursor = self.handle.cursor.return_value
        self.mock_execute = self.cursor.execute
        self.patcher = mock.patch(
            'dbt.adapters.snowflake.connections.snowflake.connector.connect')
        self.snowflake = self.patcher.start()

        self.snowflake.return_value = self.handle
        self.adapter = SnowflakeAdapter(self.config)
        # patch our new adapter into the factory so macros behave
        inject_adapter('snowflake', self.adapter)

    def tearDown(self):
        # we want a unique self.handle every time.
        self.adapter.cleanup_connections()
        self.patcher.stop()

    def test_quoting_on_drop_schema(self):
        self.adapter.drop_schema(
            database='test_database',
            schema='test_schema'
        )

        self.mock_execute.assert_has_calls([
            mock.call('drop schema if exists test_database."test_schema" cascade', None)
        ])

    def test_quoting_on_drop(self):
        relation = self.adapter.Relation.create(
            database='test_database',
            schema='test_schema',
            identifier='test_table',
            type='table',
            quote_policy=self.adapter.config.quoting,
        )
        self.adapter.drop_relation(relation)

        self.mock_execute.assert_has_calls([
            mock.call(
                'drop table if exists test_database."test_schema".test_table cascade',
                None
            )
        ])

    def test_quoting_on_truncate(self):
        relation = self.adapter.Relation.create(
            database='test_database',
            schema='test_schema',
            identifier='test_table',
            type='table',
            quote_policy=self.adapter.config.quoting,
        )
        self.adapter.truncate_relation(relation)

        self.mock_execute.assert_has_calls([
            mock.call('truncate table test_database."test_schema".test_table', None)
        ])

    def test_quoting_on_rename(self):
        from_relation = self.adapter.Relation.create(
            database='test_database',
            schema='test_schema',
            identifier='table_a',
            type='table',
            quote_policy=self.adapter.config.quoting,
        )
        to_relation = self.adapter.Relation.create(
            database='test_database',
            schema='test_schema',
            identifier='table_b',
            type='table',
            quote_policy=self.adapter.config.quoting,
        )

        self.adapter.rename_relation(
            from_relation=from_relation,
            to_relation=to_relation
        )
        self.mock_execute.assert_has_calls([
            mock.call(
                'alter table test_database."test_schema".table_a rename to test_database."test_schema".table_b',
                None
            )
        ])

    def test_cancel_open_connections_empty(self):
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_master(self):
        self.adapter.connections.in_use['master'] = mock.MagicMock()
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_single(self):
        master = mock.MagicMock()
        model = mock.MagicMock()
        model.handle.session_id = 42

        self.adapter.connections.in_use.update({
            'master': master,
            'model': model,
        })
        with mock.patch.object(self.adapter.connections, 'add_query') as add_query:
            query_result = mock.MagicMock()
            add_query.return_value = (None, query_result)

            self.assertEqual(
                len(list(self.adapter.cancel_open_connections())), 1)

            add_query.assert_called_once_with(
                'select system$abort_session(42)', 'master')

    def test_client_session_keep_alive_false_by_default(self):
        self.adapter.connections.get(name='new_connection_with_new_config')
        self.snowflake.assert_has_calls([
            mock.call(
                account='test_account', autocommit=False,
                client_session_keep_alive=False, database='test_database',
                role=None, schema='public', user='test_user',
                warehouse='test_warehouse', private_key=None)
        ])

    def test_client_session_keep_alive_true(self):
        self.config.credentials = self.config.credentials.incorporate(
            client_session_keep_alive=True)
        self.adapter = SnowflakeAdapter(self.config)
        self.adapter.connections.get(name='new_connection_with_new_config')

        self.snowflake.assert_has_calls([
            mock.call(
                account='test_account', autocommit=False,
                client_session_keep_alive=True, database='test_database',
                role=None, schema='public', user='test_user',
                warehouse='test_warehouse', private_key=None)
        ])

    def test_user_pass_authentication(self):
        self.config.credentials = self.config.credentials.incorporate(
            password='test_password')
        self.adapter = SnowflakeAdapter(self.config)
        self.adapter.connections.get(name='new_connection_with_new_config')

        self.snowflake.assert_has_calls([
            mock.call(
                account='test_account', autocommit=False,
                client_session_keep_alive=False, database='test_database',
                password='test_password', role=None, schema='public',
                user='test_user', warehouse='test_warehouse', private_key=None)
        ])

    def test_authenticator_user_pass_authentication(self):
        self.config.credentials = self.config.credentials.incorporate(
            password='test_password', authenticator='test_sso_url')
        self.adapter = SnowflakeAdapter(self.config)
        self.adapter.connections.get(name='new_connection_with_new_config')

        self.snowflake.assert_has_calls([
            mock.call(
                account='test_account', autocommit=False,
                client_session_keep_alive=False, database='test_database',
                password='test_password', role=None, schema='public',
                user='test_user', warehouse='test_warehouse',
                authenticator='test_sso_url', private_key=None)
        ])

    def test_authenticator_externalbrowser_authentication(self):
        self.config.credentials = self.config.credentials.incorporate(
            authenticator='externalbrowser')
        self.adapter = SnowflakeAdapter(self.config)
        self.adapter.connections.get(name='new_connection_with_new_config')

        self.snowflake.assert_has_calls([
            mock.call(
                account='test_account', autocommit=False,
                client_session_keep_alive=False, database='test_database',
                role=None, schema='public', user='test_user',
                warehouse='test_warehouse', authenticator='externalbrowser',
                private_key=None)
        ])

    @patch('dbt.adapters.snowflake.SnowflakeConnectionManager._get_private_key', return_value='test_key')
    def test_authenticator_private_key_authentication(self, mock_get_private_key):
        self.config.credentials = self.config.credentials.incorporate(
            private_key_path='/tmp/test_key.p8',
            private_key_passphrase='p@ssphr@se')

        self.adapter = SnowflakeAdapter(self.config)
        self.adapter.connections.get(name='new_connection_with_new_config')

        self.snowflake.assert_has_calls([
            mock.call(
                account='test_account', autocommit=False,
                client_session_keep_alive=False, database='test_database',
                role=None, schema='public', user='test_user',
                warehouse='test_warehouse', private_key='test_key')
        ])
