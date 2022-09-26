import os
import unittest
from unittest import mock
import dbt
from dbt.contracts.files import FileHash
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.model_config import NodeConfig
from dbt.contracts.graph.parsed import ParsedModelNode
from dbt.contracts.results import RunningStatus
from dbt.lib import SqlCompileRunnerNoIntrospection, compile_sql
from dbt.adapters.snowflake import Plugin
from dbt.node_types import NodeType

from test.unit.utils import clear_plugin, config_from_parts_or_dicts, inject_adapter


class MockContext():
    node = mock.MagicMock()
    node._event_status = {
        "node_status": RunningStatus.Started
    }
    node.is_ephemeral_model = True
    timing = []


class TestSqlCompileRunnerNoIntrospection(unittest.TestCase):
    def setUp(self):
            self.ctx = MockContext()
            self.manifest = {'mock':'manifest'}
            self.adapter = Plugin.adapter({})
            self.adapter.connection_for = mock.MagicMock()
            inject_adapter(self.adapter, Plugin)

    def tearDown(self):
        clear_plugin(Plugin)

    def test__compile_and_execute__with_connection(self):
        """
        By default, env var for allowing introspection is true, and calling this
        method should defer to the parent method.
        """
        with mock.patch('dbt.task.sql.GenericSqlRunner.compile') as mock_compile:
            runner = SqlCompileRunnerNoIntrospection({}, self.adapter, None, 1, 1)
            runner.compile_and_execute(self.manifest, self.ctx)
            mock_compile.assert_called_once_with(self.manifest)
            self.adapter.connection_for.assert_called_once()

    
    def test__compile_and_execute__without_connection(self):
        """
        This tests only that the proper compile_and_execute is called if introspection is disabled
        """
        with mock.patch.dict(os.environ, {"__DBT_ALLOW_INTROSPECTION": "0"}):
            with mock.patch('dbt.task.sql.GenericSqlRunner.compile') as mock_compile:
                runner = SqlCompileRunnerNoIntrospection({}, self.adapter, None, 1, 1)
                runner.compile_and_execute(self.manifest, self.ctx)
                self.adapter.connection_for.assert_not_called()
                mock_compile.assert_called_once_with(self.manifest)
