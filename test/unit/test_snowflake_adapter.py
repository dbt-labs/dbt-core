import unittest
from contextlib import contextmanager
from unittest import mock

import dbt.flags as flags

import dbt.parser.manifest
from dbt.adapters.snowflake import SnowflakeAdapter
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from dbt.parser.results import ParseResult
from snowflake import connector as snowflake_connector

from .utils import config_from_parts_or_dicts, inject_adapter, mock_connection


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
            },
            'query-comment': 'dbt',
        }
        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self.assertEqual(self.config.query_comment, 'dbt')

        self.handle = mock.MagicMock(
            spec=snowflake_connector.SnowflakeConnection)
        self.cursor = self.handle.cursor.return_value
        self.mock_execute = self.cursor.execute
        self.patcher = mock.patch(
            'dbt.adapters.snowflake.connections.snowflake.connector.connect'
        )
        self.snowflake = self.patcher.start()

        self.load_patch = mock.patch('dbt.parser.manifest.make_parse_result')
        self.mock_parse_result = self.load_patch.start()
        self.mock_parse_result.return_value = ParseResult.rpc()

        self.snowflake.return_value = self.handle
        self.adapter = SnowflakeAdapter(self.config)

        self.qh_patch = mock.patch.object(self.adapter.connections.query_header, 'add')
        self.mock_query_header_add = self.qh_patch.start()
        self.mock_query_header_add.side_effect = lambda q: '/* dbt */\n{}'.format(q)

        self.adapter.acquire_connection()
        inject_adapter(self.adapter)

    def tearDown(self):
        # we want a unique self.handle every time.
        self.adapter.cleanup_connections()
        self.qh_patch.stop()
        self.patcher.stop()
        self.load_patch.stop()

    def test_quoting_on_drop_schema(self):
        self.adapter.drop_schema(
            database='test_database',
            schema='test_schema'
        )

        self.mock_execute.assert_has_calls([
            mock.call('/* dbt */\ndrop schema if exists test_database."test_schema" cascade', None)
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
                '/* dbt */\ndrop table if exists test_database."test_schema".test_table cascade',
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
            mock.call('/* dbt */\ntruncate table test_database."test_schema".test_table', None)
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
                '/* dbt */\nalter table test_database."test_schema".table_a rename to test_database."test_schema".table_b',
                None
            )
        ])

    @contextmanager
    def current_warehouse(self, response):
        # there is probably some elegant way built into mock.patch to do this
        fetchall_return = self.cursor.fetchall.return_value
        execute_side_effect = self.mock_execute.side_effect

        def execute_effect(sql, *args, **kwargs):
            if sql == '/* dbt */\nselect current_warehouse() as warehouse':
                self.cursor.description = [['name']]
                self.cursor.fetchall.return_value = [[response]]
            else:
                self.cursor.description = None
                self.cursor.fetchall.return_value = fetchall_return
            return self.mock_execute.return_value

        self.mock_execute.side_effect = execute_effect
        try:
            yield
        finally:
            self.cursor.fetchall.return_value = fetchall_return
            self.mock_execute.side_effect = execute_side_effect

    def _strip_transactions(self):
        result = []
        for call_args in self.mock_execute.call_args_list:
            args, kwargs = tuple(call_args)
            is_transactional = (
                len(kwargs) == 0 and
                len(args) == 2 and
                args[1] is None and
                args[0] in {'BEGIN', 'COMMIT'}
            )
            if not is_transactional:
                result.append(call_args)
        return result

    def test_pre_post_hooks_warehouse(self):
        with self.current_warehouse('warehouse'):
            config = {'snowflake_warehouse': 'other_warehouse'}
            result = self.adapter.pre_model_hook(config)
            self.assertIsNotNone(result)
            calls = [
                mock.call('/* dbt */\nselect current_warehouse() as warehouse', None),
                mock.call('/* dbt */\nuse warehouse other_warehouse', None)
            ]
            self.mock_execute.assert_has_calls(calls)
            self.adapter.post_model_hook(config, result)
            calls.append(mock.call('/* dbt */\nuse warehouse warehouse', None))
            self.mock_execute.assert_has_calls(calls)

    def test_pre_post_hooks_no_warehouse(self):
        with self.current_warehouse('warehouse'):
            config = {}
            result = self.adapter.pre_model_hook(config)
            self.assertIsNone(result)
            self.mock_execute.assert_not_called()
            self.adapter.post_model_hook(config, result)
            self.mock_execute.assert_not_called()

    def test_cancel_open_connections_empty(self):
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_master(self):
        key = self.adapter.connections.get_thread_identifier()
        self.adapter.connections.thread_connections[key] = mock_connection('master')
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_single(self):
        master = mock_connection('master')
        model = mock_connection('model')
        model.handle.session_id = 42

        key = self.adapter.connections.get_thread_identifier()
        self.adapter.connections.thread_connections.update({
            key: master,
            1: model,
        })
        with mock.patch.object(self.adapter.connections, 'add_query') as add_query:
            query_result = mock.MagicMock()
            add_query.return_value = (None, query_result)

            self.assertEqual(
                len(list(self.adapter.cancel_open_connections())), 1)

            add_query.assert_called_once_with('select system$abort_session(42)')

    def test_client_session_keep_alive_false_by_default(self):
        conn = self.adapter.connections.set_connection_name(name='new_connection_with_new_config')

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls([
            mock.call(
                account='test_account', autocommit=False,
                client_session_keep_alive=False, database='test_database',
                role=None, schema='public', user='test_user',
                warehouse='test_warehouse', private_key=None, application='dbt')
        ])

    def test_client_session_keep_alive_true(self):
        self.config.credentials = self.config.credentials.replace(
                                          client_session_keep_alive=True)
        self.adapter = SnowflakeAdapter(self.config)
        conn = self.adapter.connections.set_connection_name(name='new_connection_with_new_config')

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls([
            mock.call(
                account='test_account', autocommit=False,
                client_session_keep_alive=True, database='test_database',
                role=None, schema='public', user='test_user',
                warehouse='test_warehouse', private_key=None, application='dbt')
        ])

    def test_user_pass_authentication(self):
        self.config.credentials = self.config.credentials.replace(
            password='test_password',
        )
        self.adapter = SnowflakeAdapter(self.config)
        conn = self.adapter.connections.set_connection_name(name='new_connection_with_new_config')

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls([
            mock.call(
                account='test_account', autocommit=False,
                client_session_keep_alive=False, database='test_database',
                password='test_password', role=None, schema='public',
                user='test_user', warehouse='test_warehouse', private_key=None,
                application='dbt')
        ])

    def test_authenticator_user_pass_authentication(self):
        self.config.credentials = self.config.credentials.replace(
            password='test_password',
            authenticator='test_sso_url',
        )
        self.adapter = SnowflakeAdapter(self.config)
        conn = self.adapter.connections.set_connection_name(name='new_connection_with_new_config')

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls([
            mock.call(
                account='test_account', autocommit=False,
                client_session_keep_alive=False, database='test_database',
                password='test_password', role=None, schema='public',
                user='test_user', warehouse='test_warehouse',
                authenticator='test_sso_url', private_key=None,
                application='dbt')
        ])

    def test_authenticator_externalbrowser_authentication(self):
        self.config.credentials = self.config.credentials.replace(
            authenticator='externalbrowser'
        )
        self.adapter = SnowflakeAdapter(self.config)
        conn = self.adapter.connections.set_connection_name(name='new_connection_with_new_config')

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls([
            mock.call(
                account='test_account', autocommit=False,
                client_session_keep_alive=False, database='test_database',
                role=None, schema='public', user='test_user',
                warehouse='test_warehouse', authenticator='externalbrowser',
                private_key=None, application='dbt')
        ])

    def test_authenticator_oauth_authentication(self):
        self.config.credentials = self.config.credentials.replace(
            authenticator='oauth',
            token='my-oauth-token',
        )
        self.adapter = SnowflakeAdapter(self.config)
        conn = self.adapter.connections.set_connection_name(name='new_connection_with_new_config')

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls([
            mock.call(
                account='test_account', autocommit=False,
                client_session_keep_alive=False, database='test_database',
                role=None, schema='public', user='test_user',
                warehouse='test_warehouse', authenticator='oauth', token='my-oauth-token',
                private_key=None, application='dbt')
        ])

    @mock.patch('dbt.adapters.snowflake.SnowflakeCredentials._get_private_key', return_value='test_key')
    def test_authenticator_private_key_authentication(self, mock_get_private_key):
        self.config.credentials = self.config.credentials.replace(
            private_key_path='/tmp/test_key.p8',
            private_key_passphrase='p@ssphr@se',
        )

        self.adapter = SnowflakeAdapter(self.config)
        conn = self.adapter.connections.set_connection_name(name='new_connection_with_new_config')

        self.snowflake.assert_not_called()
        conn.handle
        self.snowflake.assert_has_calls([
            mock.call(
                account='test_account', autocommit=False,
                client_session_keep_alive=False, database='test_database',
                role=None, schema='public', user='test_user',
                warehouse='test_warehouse', private_key='test_key',
                application='dbt')
        ])
