import os
import unittest
from unittest import mock
from dbt.contracts.results import RunningStatus
from dbt.lib import compile_sql
from dbt.adapters.postgres import Plugin

from test.unit.utils import clear_plugin, inject_adapter


class MockContext:
    def __init__(self, node):
        self.timing = []
        self.node = mock.MagicMock()
        self.node._event_status = {
            "node_status": RunningStatus.Started
        }
        self.node.is_ephemeral_model = True

def noop_ephemeral_result(*args):
    return None

class TestSqlCompileRunnerNoIntrospection(unittest.TestCase):
    def setUp(self):
            self.manifest = {'mock':'manifest'}
            self.adapter = Plugin.adapter({})
            self.adapter.connection_for = mock.MagicMock()
            self.ephemeral_result = lambda: None
            inject_adapter(self.adapter, Plugin)

    def tearDown(self):
        clear_plugin(Plugin)

    @mock.patch('dbt.lib._get_operation_node')
    @mock.patch('dbt.task.sql.GenericSqlRunner.compile')
    @mock.patch('dbt.task.sql.GenericSqlRunner.ephemeral_result', noop_ephemeral_result)
    @mock.patch('dbt.task.base.ExecutionContext', MockContext)
    def test__compile_and_execute__with_connection(self, mock_compile, mock_get_node):
        """
        By default, env var for allowing introspection is true, and calling this
        method should defer to the parent method.
        """
        mock_get_node.return_value = ({}, None, self.adapter)
        compile_sql(self.manifest, 'some/path', None)

        mock_compile.assert_called_once_with(self.manifest)
        self.adapter.connection_for.assert_called_once()


    @mock.patch('dbt.lib._get_operation_node')
    @mock.patch('dbt.task.sql.GenericSqlRunner.compile')
    @mock.patch('dbt.task.sql.GenericSqlRunner.ephemeral_result', noop_ephemeral_result)
    @mock.patch('dbt.task.base.ExecutionContext', MockContext)
    def test__compile_and_execute__without_connection(self, mock_compile, mock_get_node):
        """
        Ensure that compile is called but does not attempt warehouse connection
        """
        with mock.patch.dict(os.environ, {"__DBT_ALLOW_INTROSPECTION": "0"}):
            mock_get_node.return_value = ({}, None, self.adapter)
            compile_sql(self.manifest, 'some/path', None)

            mock_compile.assert_called_once_with(self.manifest)
            self.adapter.connection_for.assert_not_called()
