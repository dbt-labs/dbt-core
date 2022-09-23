import os
import unittest
from unittest import mock
from dbt.contracts.results import RunningStatus
from dbt.lib import SqlCompileRunnerNoIntrospection


class TestContext():
    node = mock.MagicMock()
    node._event_status = {
        "node_status": RunningStatus.Started
    }
    node.is_ephemeral_model = True
    timing = []


class SqlCompileRunnerNoIntrospectionTest(unittest.TestCase):
    def setUp(self):
            self.ctx = TestContext()
            self.manifest = {'mock':'data'}

    def test__compile_and_execute__with_connection(self):
        """
        By default, env var for allowing introspection is true, and calling this
        method should defer to the parent method.
        """
        with mock.patch('dbt.task.base.BaseRunner.compile_and_execute') as parent_compile:
            runner = SqlCompileRunnerNoIntrospection(None, None, None, 1, 1)
            runner.compile_and_execute(self.manifest, self.ctx)
            parent_compile.assert_called_once_with(self.manifest, self.ctx)

    def test__compile_and_execute__without_connection(self):
        """
        This tests only that the proper compile_and_execute is called if introspection is disabled
        """
        with mock.patch.dict(os.environ, {"__DBT_ALLOW_INTROSPECTION": "0"}):
            with mock.patch('dbt.task.base.BaseRunner.compile_and_execute') as mock_parent_compile:
                with mock.patch('dbt.task.sql.GenericSqlRunner.compile') as mock_sql_compile:
                    runner = SqlCompileRunnerNoIntrospection(None, None, None, 1, 1)
                    runner.compile_and_execute(self.manifest, self.ctx)
                    mock_parent_compile.assert_not_called()
                    mock_sql_compile.assert_called_once_with(self.manifest)
