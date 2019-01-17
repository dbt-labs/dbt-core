import unittest

import os

import dbt.flags
import dbt.compilation
from collections import OrderedDict
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.compiled import CompiledNode

class CompilerTest(unittest.TestCase):

    def assertEqualIgnoreWhitespace(self, a, b):
        self.assertEqual(
            "".join(a.split()),
            "".join(b.split()))

    def setUp(self):
        dbt.flags.STRICT_MODE = True

        self.maxDiff = None

        self.root_project_config = {
            'name': 'root_project',
            'version': '0.1',
            'profile': 'test',
            'project-root': os.path.abspath('.'),
        }

        self.snowplow_project_config = {
            'name': 'snowplow',
            'version': '0.1',
            'project-root': os.path.abspath('./dbt_modules/snowplow'),
        }

        self.model_config = {
            'enabled': True,
            'materialized': 'view',
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
            'quoting': {},
            'column_types': {},
            'tags': [],
        }

    def test__prepend_ctes__already_has_cte(self):
        ephemeral_config = self.model_config.copy()
        ephemeral_config['materialized'] = 'ephemeral'

        input_graph = Manifest(
            macros={},
            nodes={
                'model.root.view': CompiledNode(
                    name='view',
                    database='dbt',
                    schema='analytics',
                    alias='view',
                    resource_type='model',
                    unique_id='model.root.view',
                    fqn=['root_project', 'view'],
                    empty=False,
                    package_name='root',
                    root_path='/usr/src/app',
                    refs=[],
                    depends_on={
                        'nodes': [
                            'model.root.ephemeral'
                        ],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='view.sql',
                    original_file_path='view.sql',
                    raw_sql='select * from {{ref("ephemeral")}}',
                    compiled=True,
                    extra_ctes_injected=False,
                    extra_ctes=[
                        {'id': 'model.root.ephemeral', 'sql': None}
                    ],
                    injected_sql='',
                    compiled_sql=(
                        'with cte as (select * from something_else) '
                        'select * from __dbt__CTE__ephemeral')
                ),
                'model.root.ephemeral': CompiledNode(
                    name='ephemeral',
                    database='dbt',
                    schema='analytics',
                    alias='view',
                    resource_type='model',
                    unique_id='model.root.ephemeral',
                    fqn=['root_project', 'ephemeral'],
                    empty=False,
                    package_name='root',
                    root_path='/usr/src/app',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=ephemeral_config,
                    tags=[],
                    path='ephemeral.sql',
                    original_file_path='ephemeral.sql',
                    raw_sql='select * from source_table',
                    compiled=True,
                    compiled_sql='select * from source_table',
                    extra_ctes_injected=False,
                    extra_ctes=[],
                    injected_sql=''
                ),
            },
            docs={},
            generated_at='2018-02-14T09:15:13Z',
            disabled=[]
        )

        result, output_graph = dbt.compilation.prepend_ctes(
            input_graph.nodes['model.root.view'],
            input_graph)

        self.assertEqual(result, output_graph.nodes['model.root.view'])
        self.assertEqual(result.get('extra_ctes_injected'), True)
        self.assertEqualIgnoreWhitespace(
            result.get('injected_sql'),
            ('with __dbt__CTE__ephemeral as ('
             'select * from source_table'
             '), cte as (select * from something_else) '
             'select * from __dbt__CTE__ephemeral'))

        self.assertEqual(
            (input_graph.nodes
             .get('model.root.ephemeral', {})
             .get('extra_ctes_injected')),
            True)

    def test__prepend_ctes__no_ctes(self):
        input_graph = Manifest(
            macros={},
            nodes={
                'model.root.view': CompiledNode(
                    name='view',
                    database='dbt',
                    schema='analytics',
                    alias='view',
                    resource_type='model',
                    unique_id='model.root.view',
                    fqn=['root_project', 'view'],
                    empty=False,
                    package_name='root',
                    root_path='/usr/src/app',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='view.sql',
                    original_file_path='view.sql',
                    raw_sql=('with cte as (select * from something_else) '
                                'select * from source_table'),
                    compiled=True,
                    extra_ctes_injected=False,
                    extra_ctes=[],
                    injected_sql='',
                    compiled_sql=('with cte as (select * from something_else) '
                                     'select * from source_table')
                ),
                'model.root.view_no_cte': CompiledNode(
                    name='view_no_cte',
                    database='dbt',
                    schema='analytics',
                    alias='view_no_cte',
                    resource_type='model',
                    unique_id='model.root.view_no_cte',
                    fqn=['root_project', 'view_no_cte'],
                    empty=False,
                    package_name='root',
                    root_path='/usr/src/app',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='view.sql',
                    original_file_path='view.sql',
                    raw_sql='select * from source_table',
                    compiled=True,
                    extra_ctes_injected=False,
                    extra_ctes=[],
                    injected_sql='',
                    compiled_sql=('select * from source_table')
                ),
            },
            docs={},
            generated_at='2018-02-14T09:15:13Z',
            disabled=[]
        )

        result, output_graph = dbt.compilation.prepend_ctes(
            input_graph.nodes.get('model.root.view'),
            input_graph)

        self.assertEqual(
            result,
            output_graph.nodes.get('model.root.view'))
        self.assertEqual(result.get('extra_ctes_injected'), True)
        self.assertEqualIgnoreWhitespace(
            result.get('injected_sql'),
            (output_graph.nodes
                         .get('model.root.view')
                         .get('compiled_sql')))

        result, output_graph = dbt.compilation.prepend_ctes(
            input_graph.nodes.get('model.root.view_no_cte'),
            input_graph)

        self.assertEqual(
            result,
            output_graph.nodes.get('model.root.view_no_cte'))
        self.assertEqual(result.get('extra_ctes_injected'), True)
        self.assertEqualIgnoreWhitespace(
            result.get('injected_sql'),
            (output_graph.nodes
                         .get('model.root.view_no_cte')
                         .get('compiled_sql')))

    def test__prepend_ctes(self):
        ephemeral_config = self.model_config.copy()
        ephemeral_config['materialized'] = 'ephemeral'

        input_graph = Manifest(
            macros={},
            nodes={
                'model.root.view': CompiledNode(
                    name='view',
                    database='dbt',
                    schema='analytics',
                    alias='view',
                    resource_type='model',
                    unique_id='model.root.view',
                    fqn=['root_project', 'view'],
                    empty=False,
                    package_name='root',
                    root_path='/usr/src/app',
                    refs=[],
                    depends_on={
                        'nodes': [
                            'model.root.ephemeral'
                        ],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='view.sql',
                    original_file_path='view.sql',
                    raw_sql='select * from {{ref("ephemeral")}}',
                    compiled=True,
                    extra_ctes_injected=False,
                    extra_ctes=[
                        {'id': 'model.root.ephemeral', 'sql': None}
                    ],
                    injected_sql='',
                    compiled_sql='select * from __dbt__CTE__ephemeral'
                ),
                'model.root.ephemeral': CompiledNode(
                    name='ephemeral',
                    database='dbt',
                    schema='analytics',
                    alias='ephemeral',
                    resource_type='model',
                    unique_id='model.root.ephemeral',
                    fqn=['root_project', 'ephemeral'],
                    empty=False,
                    package_name='root',
                    root_path='/usr/src/app',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=ephemeral_config,
                    tags=[],
                    path='ephemeral.sql',
                    original_file_path='ephemeral.sql',
                    raw_sql='select * from source_table',
                    compiled=True,
                    extra_ctes_injected=False,
                    extra_ctes=[],
                    injected_sql='',
                    compiled_sql='select * from source_table'
                ),
            },
            docs={},
            generated_at='2018-02-14T09:15:13Z',
            disabled=[]
        )

        result, output_graph = dbt.compilation.prepend_ctes(
            input_graph.nodes.get('model.root.view'),
            input_graph)

        self.assertEqual(result,
                         (output_graph.nodes
                                      .get('model.root.view')))

        self.assertEqual(result.get('extra_ctes_injected'), True)
        self.assertEqualIgnoreWhitespace(
            result.get('injected_sql'),
            ('with __dbt__CTE__ephemeral as ('
             'select * from source_table'
             ') '
             'select * from __dbt__CTE__ephemeral'))

        self.assertEqual(
            (output_graph.nodes
                         .get('model.root.ephemeral', {})
                         .get('extra_ctes_injected')),
            True)

    def test__prepend_ctes__multiple_levels(self):
        ephemeral_config = self.model_config.copy()
        ephemeral_config['materialized'] = 'ephemeral'

        input_graph = Manifest(
            macros={},
            nodes={
                'model.root.view': CompiledNode(
                    name='view',
                    database='dbt',
                    schema='analytics',
                    alias='view',
                    resource_type='model',
                    unique_id='model.root.view',
                    fqn=['root_project', 'view'],
                    empty=False,
                    package_name='root',
                    root_path='/usr/src/app',
                    refs=[],
                    depends_on={
                        'nodes': [
                            'model.root.ephemeral'
                        ],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='view.sql',
                    original_file_path='view.sql',
                    raw_sql='select * from {{ref("ephemeral")}}',
                    compiled=True,
                    extra_ctes_injected=False,
                    extra_ctes=[
                        {'id': 'model.root.ephemeral', 'sql': None}
                    ],
                    injected_sql='',
                    compiled_sql='select * from __dbt__CTE__ephemeral'
                ),
                'model.root.ephemeral': CompiledNode(
                    name='ephemeral',
                    database='dbt',
                    schema='analytics',
                    alias='ephemeral',
                    resource_type='model',
                    unique_id='model.root.ephemeral',
                    fqn=['root_project', 'ephemeral'],
                    empty=False,
                    package_name='root',
                    root_path='/usr/src/app',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=ephemeral_config,
                    tags=[],
                    path='ephemeral.sql',
                    original_file_path='ephemeral.sql',
                    raw_sql='select * from {{ref("ephemeral_level_two")}}',
                    compiled=True,
                    extra_ctes_injected=False,
                    extra_ctes=[
                        {'id': 'model.root.ephemeral_level_two', 'sql': None}
                    ],
                    injected_sql='',
                    compiled_sql='select * from __dbt__CTE__ephemeral_level_two' # noqa
                ),
                'model.root.ephemeral_level_two': CompiledNode(
                    name='ephemeral_level_two',
                    database='dbt',
                    schema='analytics',
                    alias='ephemeral_level_two',
                    resource_type='model',
                    unique_id='model.root.ephemeral_level_two',
                    fqn=['root_project', 'ephemeral_level_two'],
                    empty=False,
                    package_name='root',
                    root_path='/usr/src/app',
                    refs=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=ephemeral_config,
                    tags=[],
                    path='ephemeral_level_two.sql',
                    original_file_path='ephemeral_level_two.sql',
                    raw_sql='select * from source_table',
                    compiled=True,
                    extra_ctes_injected=False,
                    extra_ctes=[],
                    injected_sql='',
                    compiled_sql='select * from source_table'
                ),
            },
            docs={},
            generated_at='2018-02-14T09:15:13Z',
            disabled=[]
        )

        result, output_graph = dbt.compilation.prepend_ctes(
            input_graph.nodes['model.root.view'],
            input_graph)

        self.assertEqual(result, input_graph.nodes['model.root.view'])
        self.assertEqual(result.get('extra_ctes_injected'), True)
        self.assertEqualIgnoreWhitespace(
            result.get('injected_sql'),
            ('with __dbt__CTE__ephemeral_level_two as ('
             'select * from source_table'
             '), __dbt__CTE__ephemeral as ('
             'select * from __dbt__CTE__ephemeral_level_two'
             ') '
             'select * from __dbt__CTE__ephemeral'))

        self.assertEqual(
            (output_graph.nodes
                         .get('model.root.ephemeral')
                         .get('extra_ctes_injected')),
            True)
        self.assertEqual(
            (output_graph.nodes
                         .get('model.root.ephemeral_level_two')
                         .get('extra_ctes_injected')),
            True)
