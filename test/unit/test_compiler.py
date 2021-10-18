import unittest
from unittest.mock import MagicMock, patch

import dbt.flags
import dbt.compilation
from dbt.adapters.postgres import Plugin
from dbt.contracts.files import FileHash
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import NodeConfig, DependsOn, ParsedModelNode
from dbt.contracts.graph.compiled import CompiledModelNode, InjectedCTE
from dbt.node_types import NodeType

from datetime import datetime

from .utils import inject_adapter, clear_plugin, config_from_parts_or_dicts


class CompilerTest(unittest.TestCase):
    def assertEqualIgnoreWhitespace(self, a, b):
        self.assertEqual(
            "".join(a.split()),
            "".join(b.split()))

    def setUp(self):
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
            'project-root': '/tmp/dbt/does-not-exist',
            'config-version': 2,
        }
        profile_cfg = {
            'outputs': {
                'test': {
                    'type': 'postgres',
                    'dbname': 'postgres',
                    'user': 'root',
                    'host': 'thishostshouldnotexist',
                    'pass': 'password',
                    'port': 5432,
                    'schema': 'public'
                }
            },
            'target': 'test'
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)

        self._generate_runtime_model_context_patch = patch.object(
            dbt.compilation, 'generate_runtime_model_context')
        self.mock_generate_runtime_model_context = self._generate_runtime_model_context_patch.start()

        inject_adapter(Plugin.adapter(self.config), Plugin)

        def mock_generate_runtime_model_context_meth(model, config, manifest):
            def ref(name):
                result = f'__dbt__cte__{name}'
                unique_id = f'model.root.{name}'
                model.extra_ctes.append(InjectedCTE(id=unique_id, sql=None))
                return result
            return {'ref': ref}

        self.mock_generate_runtime_model_context.side_effect = mock_generate_runtime_model_context_meth

    def tearDown(self):
        self._generate_runtime_model_context_patch.stop()
        clear_plugin(Plugin)

    def test__prepend_ctes__already_has_cte(self):
        ephemeral_config = self.model_config.replace(materialized='ephemeral')

        manifest = Manifest(
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
                    raw_sql='with cte as (select * from something_else) select * from {{ref("ephemeral")}}',
                    checksum=FileHash.from_contents(''),
                ),
                'model.root.ephemeral': ParsedModelNode(
                    name='ephemeral',
                    database='dbt',
                    schema='analytics',
                    alias='view',
                    resource_type=NodeType.Model,
                    unique_id='model.root.ephemeral',
                    fqn=['root', 'ephemeral'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=ephemeral_config,
                    path='ephemeral.sql',
                    original_file_path='ephemeral.sql',
                    raw_sql='select * from source_table',
                    checksum=FileHash.from_contents(''),
                ),
            },
            sources={},
            docs={},
            disabled=[],
            files={},
            exposures={},
            selectors={},
        )

        compiler = dbt.compilation.Compiler(self.config)
        result = compiler.compile_node(
            manifest.nodes['model.root.view'], manifest, write=False)

        self.assertEqual(result, manifest.nodes['model.root.view'])
        self.assertEqual(result.extra_ctes_injected, True)
        self.assertEqualIgnoreWhitespace(
            result.compiled_sql,
            ('with __dbt__cte__ephemeral as ('
             'select * from source_table'
             '), cte as (select * from something_else) '
             'select * from __dbt__cte__ephemeral'))
        self.assertEqual(
            manifest.nodes['model.root.ephemeral'].extra_ctes_injected,
            True)

    def test__prepend_ctes__no_ctes(self):
        manifest = Manifest(
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
                    raw_sql=('with cte as (select * from something_else) '
                             'select * from source_table'),
                    checksum=FileHash.from_contents(''),
                ),
                'model.root.view_no_cte': ParsedModelNode(
                    name='view_no_cte',
                    database='dbt',
                    schema='analytics',
                    alias='view_no_cte',
                    resource_type=NodeType.Model,
                    unique_id='model.root.view_no_cte',
                    fqn=['root', 'view_no_cte'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=self.model_config,
                    path='view.sql',
                    original_file_path='view.sql',
                    raw_sql='select * from source_table',
                    checksum=FileHash.from_contents(''),
                ),
            },
            sources={},
            docs={},
            disabled=[],
            files={},
            exposures={},
            selectors={},
        )

        compiler = dbt.compilation.Compiler(self.config)
        result = compiler.compile_node(
            manifest.nodes['model.root.view'], manifest, write=False)

        self.assertEqual(
            result,
            manifest.nodes.get('model.root.view'))
        self.assertTrue(result.extra_ctes_injected)
        self.assertEqualIgnoreWhitespace(
            result.compiled_sql,
            ('with cte as (select * from something_else) '
             'select * from source_table')
        )

        compiler = dbt.compilation.Compiler(self.config)
        result = compiler.compile_node(
            manifest.nodes['model.root.view_no_cte'], manifest, write=False)

        self.assertEqual(
            result,
            manifest.nodes.get('model.root.view_no_cte'))
        self.assertTrue(result.extra_ctes_injected)
        self.assertEqualIgnoreWhitespace(
            result.compiled_sql,
            'select * from source_table'
        )

    def test__prepend_ctes(self):
        ephemeral_config = self.model_config.replace(materialized='ephemeral')

        manifest = Manifest(
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
                    raw_sql='select * from {{ref("ephemeral")}}',
                    checksum=FileHash.from_contents(''),
                ),
                'model.root.ephemeral': ParsedModelNode(
                    name='ephemeral',
                    database='dbt',
                    schema='analytics',
                    alias='ephemeral',
                    resource_type=NodeType.Model,
                    unique_id='model.root.ephemeral',
                    fqn=['root', 'ephemeral'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=ephemeral_config,
                    path='ephemeral.sql',
                    original_file_path='ephemeral.sql',
                    raw_sql='select * from source_table',
                    checksum=FileHash.from_contents(''),
                ),
            },
            sources={},
            docs={},
            disabled=[],
            files={},
            exposures={},
            selectors={},
        )

        compiler = dbt.compilation.Compiler(self.config)
        result = compiler.compile_node(
            manifest.nodes['model.root.view'],
            manifest,
            write=False
        )
        self.assertTrue(result.extra_ctes_injected)
        self.assertEqualIgnoreWhitespace(
            result.compiled_sql,
            ('with __dbt__cte__ephemeral as ('
             'select * from source_table'
             ') '
             'select * from __dbt__cte__ephemeral'))
        self.assertTrue(
            manifest.nodes['model.root.ephemeral'].extra_ctes_injected)

    def test__prepend_ctes__cte_not_compiled(self):
        ephemeral_config = self.model_config.replace(materialized='ephemeral')
        parsed_ephemeral = ParsedModelNode(
            name='ephemeral',
            database='dbt',
            schema='analytics',
            alias='ephemeral',
            resource_type=NodeType.Model,
            unique_id='model.root.ephemeral',
            fqn=['root', 'ephemeral'],
            package_name='root',
            root_path='/usr/src/app',
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            config=ephemeral_config,
            tags=[],
            path='ephemeral.sql',
            original_file_path='ephemeral.sql',
            raw_sql='select * from source_table',
            checksum=FileHash.from_contents(''),
        )
        compiled_ephemeral = CompiledModelNode(
            name='ephemeral',
            database='dbt',
            schema='analytics',
            alias='ephemeral',
            resource_type=NodeType.Model,
            unique_id='model.root.ephemeral',
            fqn=['root', 'ephemeral'],
            package_name='root',
            root_path='/usr/src/app',
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            config=ephemeral_config,
            tags=[],
            path='ephemeral.sql',
            original_file_path='ephemeral.sql',
            raw_sql='select * from source_table',
            compiled=True,
            compiled_sql='select * from source_table',
            extra_ctes_injected=True,
            extra_ctes=[],
            checksum=FileHash.from_contents(''),
        )
        manifest = Manifest(
            macros={},
            nodes={
                'model.root.view': CompiledModelNode(
                    name='view',
                    database='dbt',
                    schema='analytics',
                    alias='view',
                    resource_type=NodeType.Model,
                    unique_id='model.root.view',
                    fqn=['root', 'view'],
                    package_name='root',
                    root_path='/usr/src/app',
                    refs=[],
                    sources=[],
                    depends_on=DependsOn(nodes=['model.root.ephemeral']),
                    config=self.model_config,
                    tags=[],
                    path='view.sql',
                    original_file_path='view.sql',
                    raw_sql='select * from {{ref("ephemeral")}}',
                    compiled=True,
                    extra_ctes_injected=False,
                    extra_ctes=[InjectedCTE(
                        id='model.root.ephemeral', sql='select * from source_table')],
                    compiled_sql='select * from __dbt__cte__ephemeral',
                    checksum=FileHash.from_contents(''),
                ),
                'model.root.ephemeral': parsed_ephemeral,
            },
            sources={},
            docs={},
            disabled=[],
            files={},
            exposures={},
            selectors={},
        )

        compiler = dbt.compilation.Compiler(self.config)
        with patch.object(compiler, '_compile_node') as compile_node:
            compile_node.return_value = compiled_ephemeral

            result, _ = compiler._recursively_prepend_ctes(
                manifest.nodes['model.root.view'],
                manifest,
                {}
            )
            compile_node.assert_called_once_with(
                parsed_ephemeral, manifest, {})

        self.assertEqual(result,
                         manifest.nodes.get('model.root.view'))

        self.assertTrue(manifest.nodes['model.root.ephemeral'].compiled)
        self.assertTrue(result.extra_ctes_injected)
        self.assertEqualIgnoreWhitespace(
            result.compiled_sql,
            ('with __dbt__cte__ephemeral as ('
             'select * from source_table'
             ') '
             'select * from __dbt__cte__ephemeral'))

        self.assertTrue(
            manifest.nodes['model.root.ephemeral'].extra_ctes_injected)

    def test__prepend_ctes__multiple_levels(self):
        ephemeral_config = self.model_config.replace(materialized='ephemeral')

        manifest = Manifest(
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
                    raw_sql='select * from {{ref("ephemeral")}}',
                    checksum=FileHash.from_contents(''),

                ),
                'model.root.ephemeral': ParsedModelNode(
                    name='ephemeral',
                    database='dbt',
                    schema='analytics',
                    alias='ephemeral',
                    resource_type=NodeType.Model,
                    unique_id='model.root.ephemeral',
                    fqn=['root', 'ephemeral'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=ephemeral_config,
                    path='ephemeral.sql',
                    original_file_path='ephemeral.sql',
                    raw_sql='select * from {{ref("ephemeral_level_two")}}',
                    checksum=FileHash.from_contents(''),
                ),
                'model.root.ephemeral_level_two': ParsedModelNode(
                    name='ephemeral_level_two',
                    database='dbt',
                    schema='analytics',
                    alias='ephemeral_level_two',
                    resource_type=NodeType.Model,
                    unique_id='model.root.ephemeral_level_two',
                    fqn=['root', 'ephemeral_level_two'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=ephemeral_config,
                    path='ephemeral_level_two.sql',
                    original_file_path='ephemeral_level_two.sql',
                    raw_sql='select * from source_table',
                    checksum=FileHash.from_contents(''),
                ),
            },
            sources={},
            docs={},
            disabled=[],
            files={},
            exposures={},
            selectors={},
        )

        compiler = dbt.compilation.Compiler(self.config)
        result = compiler.compile_node(manifest.nodes['model.root.view'], manifest, write=False)

        self.assertEqual(result, manifest.nodes['model.root.view'])
        self.assertTrue(result.extra_ctes_injected)
        self.assertEqualIgnoreWhitespace(
            result.compiled_sql,
            ('with __dbt__cte__ephemeral_level_two as ('
             'select * from source_table'
             '), __dbt__cte__ephemeral as ('
             'select * from __dbt__cte__ephemeral_level_two'
             ') '
             'select * from __dbt__cte__ephemeral'))

        self.assertTrue(manifest.nodes['model.root.ephemeral'].compiled)
        self.assertTrue(
            manifest.nodes['model.root.ephemeral_level_two'].compiled)
        self.assertTrue(
            manifest.nodes['model.root.ephemeral'].extra_ctes_injected)
        self.assertTrue(
            manifest.nodes['model.root.ephemeral_level_two'].extra_ctes_injected)

    def test__prepend_ctes__valid_ephemeral_sql(self):
        """Assert that the compiled sql for ephemeral models is valid and can be executed on its own"""
        ephemeral_config = self.model_config.replace(materialized='ephemeral')

        manifest = Manifest(
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
                    raw_sql='select * from {{ref("ephemeral")}}',
                    checksum=FileHash.from_contents(''),
                ),
                'model.root.inner_ephemeral': ParsedModelNode(
                    name='inner_ephemeral',
                    database='dbt',
                    schema='analytics',
                    alias='inner_ephemeral',
                    resource_type=NodeType.Model,
                    unique_id='model.root.inner_ephemeral',
                    fqn=['root', 'inner_ephemeral'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=ephemeral_config,
                    path='inner_ephemeral.sql',
                    original_file_path='inner_ephemeral.sql',
                    raw_sql='select * from source_table',
                    checksum=FileHash.from_contents(''),
                ),
                 'model.root.ephemeral': ParsedModelNode(
                    name='ephemeral',
                    database='dbt',
                    schema='analytics',
                    alias='ephemeral',
                    resource_type=NodeType.Model,
                    unique_id='model.root.ephemeral',
                    fqn=['root', 'ephemeral'],
                    package_name='root',
                    root_path='/usr/src/app',
                    config=ephemeral_config,
                    path='ephemeral.sql',
                    original_file_path='ephemeral.sql',
                    raw_sql='select * from {{ ref("inner_ephemeral") }}',
                    checksum=FileHash.from_contents(''),
                ),
            },
            sources={},
            docs={},
            disabled=[],
            files={},
            exposures={},
            selectors={},
        )

        compiler = dbt.compilation.Compiler(self.config)
        result = compiler.compile_node(
            manifest.nodes['model.root.view'],
            manifest,
            write=False
        )
        self.assertEqualIgnoreWhitespace(
            result.compiled_sql,
            ('with __dbt__cte__inner_ephemeral as ('
             'select * from source_table'
             '), '
             '__dbt__cte__ephemeral as ('
             'select * from __dbt__cte__inner_ephemeral'
             ') '
             'select * from __dbt__cte__ephemeral'))
        self.assertEqualIgnoreWhitespace(
            manifest.nodes['model.root.ephemeral'].compiled_sql,
            ('with __dbt__cte__inner_ephemeral as ('
            'select * from source_table'
            ')  '
            'select * from __dbt__cte__inner_ephemeral')
        )

