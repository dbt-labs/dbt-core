import os
import unittest
from unittest import mock
import dbt
from dbt.contracts.files import FileHash
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.model_config import NodeConfig
from dbt.contracts.graph.parsed import ParsedModelNode
from dbt.contracts.results import RunningStatus
from dbt.lib import SqlCompileRunnerNoWarehouseConnection, compile_sql
from dbt.adapters.snowflake import Plugin
from dbt.node_types import NodeType

from test.unit.utils import clear_plugin, config_from_parts_or_dicts, inject_adapter


class TestContext():
    node = mock.MagicMock()
    node._event_status = {
        "node_status": RunningStatus.Started
    }
    node.is_ephemeral_model = True
    timing = []


class SqlCompileRunnerNoWarehouseConnectionTest(unittest.TestCase):
    def setUp(self):
            self.ctx = TestContext()
            self.maxDiff = None

            self.model_config = NodeConfig.from_dict({
                'enabled': True,
                'materialized': 'view',
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'vars': {},
                'quoting': {},
                'column_types': {},
                'tags': [],
            })

            project_cfg = {
                'name': 'X',
                'version': '0.1',
                'profile': 'test',
                'project-root': '/tmp/dbt/some-test-project',
                'config-version': 2,
            }
            profile_cfg = {
                'outputs': {
                    'test': {
                        'client_session_keep_alive': False,
                        'type': 'snowflake',
                        'database': 'TEST',
                        'user': 'root',
                        'account': 'ska12345',
                        'password': 'password',
                        'role': 'TESTER',
                        'schema': 'semantic_layer',
                        'warehouse': 'test_warehouse'
                    }
                },
                'target': 'test'
            }

            self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
            self.manifest = Manifest(
              macros={},
              nodes={
                  'model.root.view': ParsedModelNode(
                      name='view',
                      database='dbt',
                      schema='analytics',
                      alias='view',
                      resource_type=NodeType.Model,
                      unique_id='model.root.view',
                      fqn=['root', 'view'],
                      package_name='root',
                      root_path='/usr/src/app',
                      config=self.model_config,
                      path='view.sql',
                      original_file_path='view.sql',
                      language='sql',
                      raw_code='with cte as (select * from something_else) select * from {{ref("ephemeral")}}',
                      checksum=FileHash.from_contents(''),
                  ),
              },
              sources={},
              docs={},
              disabled=[],
              files={},
              exposures={},
              metrics={},
              selectors={},
            )
            self.adapter = Plugin.adapter(self.config)
            inject_adapter(self.adapter, Plugin)

    def tearDown(self):
        clear_plugin(Plugin)

    def test__compile_and_execute__with_connection(self):
        """
        By default, env var for allowing introspection is true, and calling this
        method should defer to the parent method.
        """
        with mock.patch('dbt.task.base.BaseRunner.compile_and_execute') as parent_compile:
            runner = SqlCompileRunnerNoWarehouseConnection(self.config, self.adapter, self.manifest.nodes['model.root.view'], 1, 1)
            runner.compile_and_execute(self.manifest, self.ctx)
            parent_compile.assert_called_once_with(self.manifest, self.ctx)

    
    def test__compile_and_execute__without_connection(self):
        """
        This tests only that the proper compile_and_execute is called if introspection is disabled
        """
        with mock.patch.dict(os.environ, {"__DBT_ALLOW_INTROSPECTION": "0"}):
            with mock.patch('dbt.task.base.BaseRunner.compile_and_execute') as parent_compile:
                with mock.patch('dbt.task.sql.GenericSqlRunner.compile') as mock_compile:
                    runner = SqlCompileRunnerNoWarehouseConnection(self.config, self.adapter, self.manifest.nodes['model.root.view'], 1, 1)
                    runner.compile_and_execute(self.manifest, self.ctx)
                    parent_compile.assert_not_called()
                    mock_compile.assert_called_once_with(self.manifest)
