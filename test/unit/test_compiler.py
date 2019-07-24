import unittest

import os
import mock

import dbt.flags
import dbt.compilation
from dbt.config import RuntimeConfig
from dbt.adapters.base import Credentials
from collections import OrderedDict
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.compiled import CompiledNode
from dbt.contracts.graph.parsed import ParsedNode

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
            'persist_docs': {},
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
                    sources=[],
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
                    sources=[],
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
                    sources=[],
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
                    sources=[],
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
                    sources=[],
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
                    sources=[],
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
                    sources=[],
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
                    sources=[],
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
                    sources=[],
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


class Args(object):
    def __init__(self, profiles_dir=None, threads=None, profile=None,
                 cli_vars=None, version_check=None, project_dir=None):
        self.profile = profile
        if threads is not None:
            self.threads = threads
        if profiles_dir is not None:
            self.profiles_dir = profiles_dir
        if cli_vars is not None:
            self.vars = cli_vars
        if version_check is not None:
            self.version_check = version_check
        if project_dir is not None:
            self.project_dir = project_dir


class BaseCompilerRefTest(unittest.TestCase):
    """ NB this method forms the base of both the compiler
    tests here but also the adapter specific compiler tests
    which are in other test files """

    @staticmethod
    def make_runtime_config(project_data, profile_data, profiles_dir, project_dir):
        return dbt.config.RuntimeConfig.from_parts(
            dbt.config.Project.from_project_config(
                project_data),
            dbt.config.Profile.from_raw_profiles(
                profile_data, project_data['profile'],
                {}),
            Args(profiles_dir=profiles_dir, cli_vars='{}',
                version_check=True, project_dir=project_dir))

    def set_variables(self):
        # NB setUP is called for each test
        dbt.flags.STRICT_MODE = True

        self.maxDiff = None

        self.model_config = {
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
            'quoting': {},
            'column_types': {},
            'tags': []
        }

        self.profiles_dir = '/invalid-profiles-path'
        self.project_dir = '/invalid-root-path'

        self.default_project_data = {
            'version': '0.0.1',
            'name': 'my_test_project',
            'profile': 'default',
            'project-root': self.project_dir
        }

        self.quoted_project_data = self.default_project_data.copy()
        self.quoted_project_data['quoting'] = {
            # An example quoting scheme for testing.
            # Otherwise the same as the other profile.
            'database': False,
            'schema': True,
            'identifier': False
        }

        self.default_profile_data = {
            'default': {
                'outputs': {
                    'postgres': {
                        'type': 'postgres',
                        'host': 'postgres-db-hostname',
                        'port': 5555,
                        'user': 'db_user',
                        'pass': 'db_pass',
                        'dbname': 'postgres-db-name',
                        'schema': 'postgres-schema',
                        'threads': 7,
                    },
                },
                'target': 'postgres',
            }
        }

    def generate_objects(self):
        self.test_profile = self.make_runtime_config(
            self.default_project_data, self.default_profile_data,
            self.profiles_dir, self.project_dir)
        self.quoted_test_profile = self.make_runtime_config(
            self.quoted_project_data, self.default_profile_data,
            self.profiles_dir, self.project_dir)
        self.input_graph = Manifest(
            macros={},
            nodes={
                'model.test_package.root_table_a': CompiledNode(
                    name='root_table_a',
                    database='dbt',
                    schema='analytics',
                    alias='root_table_a',
                    resource_type='model',
                    unique_id='model.test_package.root_table_a',
                    fqn=['my_test_project', 'root_table_a'],
                    empty=False,
                    package_name='test_package',
                    root_path='/usr/src/app',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='root_table_a.sql',
                    original_file_path='root_table_a.sql',
                    raw_sql='select * from source_table',
                    compiled=True,
                    extra_ctes_injected=False,
                    extra_ctes=[],
                    injected_sql='',
                    compiled_sql='select * from source_table'
                ),
                'model.test_package.root_table_b': CompiledNode(
                    name='root_table_b',
                    database='dbt',
                    schema='other_analytics',
                    alias='root_table_b',
                    resource_type='model',
                    unique_id='model.test_package.root_table_b',
                    fqn=['my_test_project', 'root_table_b'],
                    empty=False,
                    package_name='test_package',
                    root_path='/usr/src/app',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='root_table_b.sql',
                    original_file_path='root_table_b.sql',
                    raw_sql='select * from another_source_table',
                    compiled=True,
                    extra_ctes_injected=False,
                    extra_ctes=[],
                    injected_sql='',
                    compiled_sql='select * from another_source_table'
                )
            },
            docs={},
            generated_at='2018-02-14T09:15:13Z',
            disabled=[]
        )

    def setUp(self):
        self.set_variables()
        self.generate_objects()

    def generate_test_node(self, sql, name='test_view', schema='analytics', database='dbt', config_override=None):
        # Provide the opportunity to override any config (used for schema level relative ref)
        config_override = config_override or {}
        self.model_config.update(config_override)

        path = name + '.sql'
        return ParsedNode(
            name=name, database=database, schema=schema,
            alias=name, resource_type='model',
            unique_id='model.test_package.' + name, fqn=['my_test_project', name],
            empty=False, package_name='test_package', root_path='/usr/src/app',
            refs=[], sources=[], config=self.model_config,
            depends_on={
                'nodes': [
                    'model.test_package.root_table_a',
                    'model.test_package.root_table_b'],
                'macros': []
            },
            tags=[], path=path, original_file_path=path, raw_sql=sql
        )

    def compile_node(self, node, profile, extra_context=None):
        extra_context = extra_context or {}
        compiler = dbt.compilation.Compiler(profile)
        return compiler.compile_node(node, self.input_graph, extra_context=extra_context)


class CompilerRefTest(BaseCompilerRefTest):

    def setUp(self):
        # Call the parent setup function
        super(CompilerRefTest, self).setUp()

    def test__simple_content(self):
        """ Test simple content replacement """
        node = self.generate_test_node('select * from {{ blah }}')
        compiled_node = self.compile_node(node, profile=self.test_profile, extra_context={'blah': 'blahblah'})
        assert compiled_node.compiled_sql == 'select * from blahblah'

    def test__simple_ref(self):
        """ Test simple ref - nothing fancy """
        node = self.generate_test_node("select * from {{ ref('root_table_a') }}")
        compiled_node = self.compile_node(node, profile=self.test_profile)
        assert compiled_node.compiled_sql == 'select * from "dbt"."analytics"."root_table_a"'

    def test__simple_ref_no_quote(self):
        """ Test quoting config """
        node = self.generate_test_node("select * from {{ ref('root_table_a') }}")
        compiled_node = self.compile_node(node, profile=self.quoted_test_profile)
        assert compiled_node.compiled_sql == 'select * from dbt."analytics".root_table_a'
