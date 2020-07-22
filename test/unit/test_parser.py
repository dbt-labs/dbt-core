import unittest
from unittest import mock

import os
import yaml

import dbt.flags
import dbt.parser
from dbt.exceptions import CompilationException
from dbt.parser import (
    ModelParser, MacroParser, DataTestParser, SchemaParser, ParseResult,
    SnapshotParser, AnalysisParser
)
from dbt.parser.schemas import (
    TestablePatchParser, SourceParser, AnalysisPatchParser, MacroPatchParser
)
from dbt.parser.search import FileBlock
from dbt.parser.schema_test_builders import YamlBlock
from dbt.parser.manifest import process_docs, process_sources, process_refs

from dbt.node_types import NodeType
from dbt.contracts.graph.manifest import (
    Manifest, FilePath, SourceFile, FileHash
)
from dbt.contracts.graph.model_config import (
    NodeConfig, TestConfig, TimestampSnapshotConfig, SnapshotStrategy,
)
from dbt.contracts.graph.parsed import (
    ParsedModelNode, ParsedMacro, ParsedNodePatch, DependsOn, ColumnInfo,
    ParsedDataTestNode, ParsedSnapshotNode, ParsedAnalysisNode,
    UnpatchedSourceDefinition
)
from dbt.contracts.graph.unparsed import Docs

from .utils import config_from_parts_or_dicts, normalize, generate_name_macros, MockNode, MockSource, MockDocumentation


def get_abs_os_path(unix_path):
    return normalize(os.path.abspath(unix_path))


class BaseParserTest(unittest.TestCase):
    maxDiff = None

    def _generate_macros(self):
        name_sql = {}
        for component in ('database', 'schema', 'alias'):
            if component == 'alias':
                source = 'node.name'
            else:
                source = f'target.{component}'
            name = f'generate_{component}_name'
            sql = f'{{% macro {name}(value, node) %}} {{% if value %}} {{{{ value }}}} {{% else %}} {{{{ {source} }}}} {{% endif %}} {{% endmacro %}}'
            name_sql[name] = sql

        for name, sql in name_sql.items():
            pm = ParsedMacro(
                name=name,
                resource_type=NodeType.Macro,
                unique_id=f'macro.root.{name}',
                package_name='root',
                original_file_path=normalize('macros/macro.sql'),
                root_path=get_abs_os_path('./dbt_modules/root'),
                path=normalize('macros/macro.sql'),
                macro_sql=sql,
            )
            yield pm

    def setUp(self):
        dbt.flags.STRICT_MODE = True
        dbt.flags.WARN_ERROR = True

        self.maxDiff = None

        profile_data = {
            'target': 'test',
            'quoting': {},
            'outputs': {
                'test': {
                    'type': 'redshift',
                    'host': 'localhost',
                    'schema': 'analytics',
                    'user': 'test',
                    'pass': 'test',
                    'dbname': 'test',
                    'port': 1,
                }
            }
        }

        root_project = {
            'name': 'root',
            'version': '0.1',
            'profile': 'test',
            'project-root': normalize('/usr/src/app'),
        }

        self.root_project_config = config_from_parts_or_dicts(
            project=root_project,
            profile=profile_data,
            cli_vars='{"test_schema_name": "foo"}'
        )

        snowplow_project = {
            'name': 'snowplow',
            'version': '0.1',
            'profile': 'test',
            'project-root': get_abs_os_path('./dbt_modules/snowplow'),
        }

        self.snowplow_project_config = config_from_parts_or_dicts(
            project=snowplow_project, profile=profile_data
        )

        self.all_projects = {
            'root': self.root_project_config,
            'snowplow': self.snowplow_project_config
        }

        self.root_project_config.dependencies = self.all_projects
        self.snowplow_project_config.dependencies = self.all_projects
        self.patcher = mock.patch('dbt.context.providers.get_adapter')
        self.factory = self.patcher.start()

        self.parser_patcher = mock.patch('dbt.parser.base.get_adapter')
        self.factory_parser = self.parser_patcher.start()

        self.macro_manifest = Manifest.from_macros(
            macros={m.unique_id: m for m in generate_name_macros('root')}
        )

    def tearDown(self):
        self.parser_patcher.stop()
        self.patcher.stop()

    def file_block_for(self, data: str, filename: str, searched: str):
        root_dir = get_abs_os_path('./dbt_modules/snowplow')
        filename = normalize(filename)
        path = FilePath(
            searched_path=searched,
            relative_path=filename,
            project_root=root_dir,
        )
        source_file = SourceFile(
            path=path,
            checksum=FileHash.from_contents(data),
        )
        source_file.contents = data
        return FileBlock(file=source_file)

    def assert_has_results_length(self, results, files=1, macros=0, nodes=0,
                                  sources=0, docs=0, patches=0, disabled=0):
        self.assertEqual(len(results.files), files)
        self.assertEqual(len(results.macros), macros)
        self.assertEqual(len(results.nodes), nodes)
        self.assertEqual(len(results.sources), sources)
        self.assertEqual(len(results.docs), docs)
        self.assertEqual(len(results.patches), patches)
        self.assertEqual(sum(len(v) for v in results.disabled.values()), disabled)


SINGLE_TABLE_SOURCE = '''
version: 2
sources:
    - name: my_source
      tables:
        - name: my_table
'''

SINGLE_TABLE_SOURCE_TESTS = '''
version: 2
sources:
    - name: my_source
      tables:
        - name: my_table
          description: A description of my table
          columns:
            - name: color
              tests:
                - not_null:
                    severity: WARN
                - accepted_values:
                    values: ['red', 'blue', 'green']
'''


SINGLE_TABLE_MODEL_TESTS = '''
version: 2
models:
    - name: my_model
      description: A description of my model
      columns:
        - name: color
          description: The color value
          tests:
            - not_null:
                severity: WARN
            - accepted_values:
                values: ['red', 'blue', 'green']
            - foreign_package.test_case:
                arg: 100
'''


SINGLE_TABLE_SOURCE_PATCH = '''
version: 2
sources:
  - name: my_source
    overrides: snowplow
    tables:
      - name: my_table
        columns:
          - name: id
            tests:
              - not_null
              - unique
'''


class SchemaParserTest(BaseParserTest):
    def setUp(self):
        super().setUp()
        self.parser = SchemaParser(
            results=ParseResult.rpc(),
            project=self.snowplow_project_config,
            root_project=self.root_project_config,
            macro_manifest=self.macro_manifest,
        )

    def file_block_for(self, data, filename):
        return super().file_block_for(data, filename, 'models')

    def yaml_block_for(self, test_yml: str, filename: str):
        file_block = self.file_block_for(data=test_yml, filename=filename)
        return YamlBlock.from_file_block(
            src=file_block,
            data=yaml.safe_load(test_yml),
        )


class SchemaParserSourceTest(SchemaParserTest):
    def test__read_basic_source(self):
        block = self.yaml_block_for(SINGLE_TABLE_SOURCE, 'test_one.yml')
        analysis_blocks = AnalysisPatchParser(self.parser, block, 'analyses').parse()
        model_blocks = TestablePatchParser(self.parser, block, 'models').parse()
        source_blocks = SourceParser(self.parser, block, 'sources').parse()
        macro_blocks = MacroPatchParser(self.parser, block, 'macros').parse()
        self.assertEqual(len(analysis_blocks), 0)
        self.assertEqual(len(model_blocks), 0)
        self.assertEqual(len(source_blocks), 0)
        self.assertEqual(len(macro_blocks), 0)
        self.assertEqual(len(list(self.parser.results.patches)), 0)
        self.assertEqual(len(list(self.parser.results.nodes)), 0)
        results = list(self.parser.results.sources.values())
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].source.name, 'my_source')
        self.assertEqual(results[0].table.name, 'my_table')
        self.assertEqual(results[0].table.description, '')
        self.assertEqual(len(results[0].table.columns), 0)

    def test__parse_basic_source(self):
        block = self.file_block_for(SINGLE_TABLE_SOURCE, 'test_one.yml')
        self.parser.parse_file(block)
        self.assert_has_results_length(self.parser.results, sources=1)
        src = list(self.parser.results.sources.values())[0]
        assert isinstance(src, UnpatchedSourceDefinition)
        assert src.package_name == 'snowplow'
        assert src.source.name == 'my_source'
        assert src.table.name == 'my_table'
        assert src.resource_type == NodeType.Source
        assert src.fqn == ['snowplow', 'my_source', 'my_table']

    def test__read_basic_source_tests(self):
        block = self.yaml_block_for(SINGLE_TABLE_SOURCE_TESTS, 'test_one.yml')
        analysis_tests = AnalysisPatchParser(self.parser, block, 'analyses').parse()
        model_tests = TestablePatchParser(self.parser, block, 'models').parse()
        source_tests = SourceParser(self.parser, block, 'sources').parse()
        macro_tests = MacroPatchParser(self.parser, block, 'macros').parse()
        self.assertEqual(len(analysis_tests), 0)
        self.assertEqual(len(model_tests), 0)
        self.assertEqual(len(source_tests), 0)
        self.assertEqual(len(macro_tests), 0)
        self.assertEqual(len(list(self.parser.results.nodes)), 0)
        self.assertEqual(len(list(self.parser.results.patches)), 0)
        self.assertEqual(len(list(self.parser.results.source_patches)), 0)
        results = list(self.parser.results.sources.values())
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].source.name, 'my_source')
        self.assertEqual(results[0].table.name, 'my_table')
        self.assertEqual(results[0].table.description, 'A description of my table')
        self.assertEqual(len(results[0].table.columns), 1)

    def test__parse_basic_source_tests(self):
        block = self.file_block_for(SINGLE_TABLE_SOURCE_TESTS, 'test_one.yml')
        self.parser.parse_file(block)
        self.assertEqual(len(self.parser.results.nodes), 0)
        self.assertEqual(len(self.parser.results.sources), 1)
        self.assertEqual(len(self.parser.results.patches), 0)
        src = list(self.parser.results.sources.values())[0]
        self.assertEqual(src.source.name, 'my_source')
        self.assertEqual(src.source.schema, None)
        self.assertEqual(src.table.name, 'my_table')
        self.assertEqual(src.table.description, 'A description of my table')

        tests = [
            self.parser.parse_source_test(src, test, col)
            for test, col in src.get_tests()
        ]
        tests.sort(key=lambda n: n.unique_id)

        self.assertEqual(tests[0].config.severity, 'ERROR')
        self.assertEqual(tests[0].tags, ['schema'])
        self.assertEqual(tests[0].sources, [['my_source', 'my_table']])
        self.assertEqual(tests[0].column_name, 'color')
        self.assertEqual(tests[0].fqn, ['snowplow', 'schema_test', tests[0].name])
        self.assertEqual(tests[1].config.severity, 'WARN')
        self.assertEqual(tests[1].tags, ['schema'])
        self.assertEqual(tests[1].sources, [['my_source', 'my_table']])
        self.assertEqual(tests[1].column_name, 'color')
        self.assertEqual(tests[1].fqn, ['snowplow', 'schema_test', tests[1].name])

        path = get_abs_os_path('./dbt_modules/snowplow/models/test_one.yml')
        self.assertIn(path, self.parser.results.files)
        self.assertEqual(self.parser.results.files[path].nodes, [])
        self.assertIn(path, self.parser.results.files)
        self.assertEqual(self.parser.results.files[path].sources,
                         ['source.snowplow.my_source.my_table'])
        self.assertEqual(self.parser.results.files[path].source_patches, [])

    def test__read_source_patch(self):
        block = self.yaml_block_for(SINGLE_TABLE_SOURCE_PATCH, 'test_one.yml')
        analysis_tests = AnalysisPatchParser(self.parser, block, 'analyses').parse()
        model_tests = TestablePatchParser(self.parser, block, 'models').parse()
        source_tests = SourceParser(self.parser, block, 'sources').parse()
        macro_tests = MacroPatchParser(self.parser, block, 'macros').parse()
        self.assertEqual(len(analysis_tests), 0)
        self.assertEqual(len(model_tests), 0)
        self.assertEqual(len(source_tests), 0)
        self.assertEqual(len(macro_tests), 0)
        self.assertEqual(len(list(self.parser.results.nodes)), 0)
        self.assertEqual(len(list(self.parser.results.patches)), 0)
        self.assertEqual(len(list(self.parser.results.sources)), 0)
        results = list(self.parser.results.source_patches.values())
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].name, 'my_source')
        self.assertEqual(results[0].overrides, 'snowplow')
        self.assertIsNone(results[0].description)
        self.assertEqual(len(results[0].tables), 1)
        table = results[0].tables[0]
        self.assertEqual(table.name, 'my_table')
        self.assertIsNone(table.description)
        self.assertEqual(len(table.columns), 1)
        self.assertEqual(len(table.columns[0].tests), 2)


class SchemaParserModelsTest(SchemaParserTest):
    def test__read_basic_model_tests(self):
        block = self.yaml_block_for(SINGLE_TABLE_MODEL_TESTS, 'test_one.yml')
        self.parser.parse_file(block)
        self.assertEqual(len(list(self.parser.results.patches)), 1)
        self.assertEqual(len(list(self.parser.results.sources)), 0)
        self.assertEqual(len(list(self.parser.results.nodes)), 3)

    def test__parse_basic_model_tests(self):
        block = self.file_block_for(SINGLE_TABLE_MODEL_TESTS, 'test_one.yml')
        self.parser.parse_file(block)
        self.assert_has_results_length(self.parser.results, patches=1, nodes=3)

        patch = list(self.parser.results.patches.values())[0]
        self.assertEqual(len(patch.columns), 1)
        self.assertEqual(patch.name, 'my_model')
        self.assertEqual(patch.description, 'A description of my model')
        expected_patch = ParsedNodePatch(
            name='my_model',
            description='A description of my model',
            columns={'color': ColumnInfo(name='color', description='The color value')},
            original_file_path=normalize('models/test_one.yml'),
            meta={},
            yaml_key='models',
            package_name='snowplow',
            docs=Docs(show=True),
        )
        self.assertEqual(patch, expected_patch)

        tests = sorted(self.parser.results.nodes.values(), key=lambda n: n.unique_id)
        self.assertEqual(tests[0].config.severity, 'ERROR')
        self.assertEqual(tests[0].tags, ['schema'])
        self.assertEqual(tests[0].refs, [['my_model']])
        self.assertEqual(tests[0].column_name, 'color')
        self.assertEqual(tests[0].package_name, 'snowplow')
        self.assertTrue(tests[0].name.startswith('accepted_values_'))
        self.assertEqual(tests[0].fqn, ['snowplow', 'schema_test', tests[0].name])
        self.assertEqual(tests[0].unique_id.split('.'), ['test', 'snowplow', tests[0].name])
        self.assertEqual(tests[0].test_metadata.name, 'accepted_values')
        self.assertIsNone(tests[0].test_metadata.namespace)
        self.assertEqual(
            tests[0].test_metadata.kwargs,
            {
                'column_name': 'color',
                'model': "{{ ref('my_model') }}",
                'values': ['red', 'blue', 'green'],
            }
        )

        # foreign packages are a bit weird, they include the macro package
        # name in the test name
        self.assertEqual(tests[1].config.severity, 'ERROR')
        self.assertEqual(tests[1].tags, ['schema'])
        self.assertEqual(tests[1].refs, [['my_model']])
        self.assertEqual(tests[1].column_name, 'color')
        self.assertEqual(tests[1].column_name, 'color')
        self.assertEqual(tests[1].fqn, ['snowplow', 'schema_test', tests[1].name])
        self.assertTrue(tests[1].name.startswith('foreign_package_test_case_'))
        self.assertEqual(tests[1].package_name, 'snowplow')
        self.assertEqual(tests[1].unique_id.split('.'), ['test', 'snowplow', tests[1].name])
        self.assertEqual(tests[1].test_metadata.name, 'test_case')
        self.assertEqual(tests[1].test_metadata.namespace, 'foreign_package')
        self.assertEqual(
            tests[1].test_metadata.kwargs,
            {
                'column_name': 'color',
                'model': "{{ ref('my_model') }}",
                'arg': 100,
            },
        )

        self.assertEqual(tests[2].config.severity, 'WARN')
        self.assertEqual(tests[2].tags, ['schema'])
        self.assertEqual(tests[2].refs, [['my_model']])
        self.assertEqual(tests[2].column_name, 'color')
        self.assertEqual(tests[2].package_name, 'snowplow')
        self.assertTrue(tests[2].name.startswith('not_null_'))
        self.assertEqual(tests[2].fqn, ['snowplow', 'schema_test', tests[2].name])
        self.assertEqual(tests[2].unique_id.split('.'), ['test', 'snowplow', tests[2].name])
        self.assertEqual(tests[2].test_metadata.name, 'not_null')
        self.assertIsNone(tests[2].test_metadata.namespace)
        self.assertEqual(
            tests[2].test_metadata.kwargs,
            {
                'column_name': 'color',
                'model': "{{ ref('my_model') }}",
            },
        )

        path = get_abs_os_path('./dbt_modules/snowplow/models/test_one.yml')
        self.assertIn(path, self.parser.results.files)
        self.assertEqual(sorted(self.parser.results.files[path].nodes),
                         [t.unique_id for t in tests])
        self.assertIn(path, self.parser.results.files)
        self.assertEqual(self.parser.results.files[path].patches, ['my_model'])


class ModelParserTest(BaseParserTest):
    def setUp(self):
        super().setUp()
        self.parser = ModelParser(
            results=ParseResult.rpc(),
            project=self.snowplow_project_config,
            root_project=self.root_project_config,
            macro_manifest=self.macro_manifest,
        )

    def file_block_for(self, data, filename):
        return super().file_block_for(data, filename, 'models')

    def test_basic(self):
        raw_sql = '{{ config(materialized="table") }}select 1 as id'
        block = self.file_block_for(raw_sql, 'nested/model_1.sql')
        self.parser.parse_file(block)
        self.assert_has_results_length(self.parser.results, nodes=1)
        node = list(self.parser.results.nodes.values())[0]
        expected = ParsedModelNode(
            alias='model_1',
            name='model_1',
            database='test',
            schema='analytics',
            resource_type=NodeType.Model,
            unique_id='model.snowplow.model_1',
            fqn=['snowplow', 'nested', 'model_1'],
            package_name='snowplow',
            original_file_path=normalize('models/nested/model_1.sql'),
            root_path=get_abs_os_path('./dbt_modules/snowplow'),
            config=NodeConfig(materialized='table'),
            path=normalize('nested/model_1.sql'),
            raw_sql=raw_sql,
        )
        self.assertEqual(node, expected)
        path = get_abs_os_path('./dbt_modules/snowplow/models/nested/model_1.sql')
        self.assertIn(path, self.parser.results.files)
        self.assertEqual(self.parser.results.files[path].nodes, ['model.snowplow.model_1'])

    def test_parse_error(self):
        block = self.file_block_for('{{ SYNTAX ERROR }}', 'nested/model_1.sql')
        with self.assertRaises(CompilationException):
            self.parser.parse_file(block)
        self.assert_has_results_length(self.parser.results, files=0)


class SnapshotParserTest(BaseParserTest):
    def setUp(self):
        super().setUp()
        self.parser = SnapshotParser(
            results=ParseResult.rpc(),
            project=self.snowplow_project_config,
            root_project=self.root_project_config,
            macro_manifest=self.macro_manifest,
        )

    def file_block_for(self, data, filename):
        return super().file_block_for(data, filename, 'snapshots')

    def test_parse_error(self):
        block = self.file_block_for('{% snapshot foo %}select 1 as id{%snapshot bar %}{% endsnapshot %}', 'nested/snap_1.sql')
        with self.assertRaises(CompilationException):
            self.parser.parse_file(block)
        self.assert_has_results_length(self.parser.results, files=0)

    def test_single_block(self):
        raw_sql = '''{{
                config(unique_key="id", target_schema="analytics",
                       target_database="dbt", strategy="timestamp",
                       updated_at="last_update")
            }}
            select 1 as id, now() as last_update'''
        full_file = '''
        {{% snapshot foo %}}{}{{% endsnapshot %}}
        '''.format(raw_sql)
        block = self.file_block_for(full_file, 'nested/snap_1.sql')
        self.parser.parse_file(block)
        self.assert_has_results_length(self.parser.results, nodes=1)
        node = list(self.parser.results.nodes.values())[0]
        expected = ParsedSnapshotNode(
            alias='foo',
            name='foo',
            # the `database` entry is overrridden by the target_database config
            database='dbt',
            schema='analytics',
            resource_type=NodeType.Snapshot,
            unique_id='snapshot.snowplow.foo',
            fqn=['snowplow', 'nested', 'snap_1', 'foo'],
            package_name='snowplow',
            original_file_path=normalize('snapshots/nested/snap_1.sql'),
            root_path=get_abs_os_path('./dbt_modules/snowplow'),
            config=TimestampSnapshotConfig(
                strategy=SnapshotStrategy.Timestamp,
                updated_at='last_update',
                target_database='dbt',
                target_schema='analytics',
                unique_key='id',
                materialized='snapshot',
            ),
            path=normalize('nested/snap_1.sql'),
            raw_sql=raw_sql,
        )
        self.assertEqual(node, expected)
        path = get_abs_os_path('./dbt_modules/snowplow/snapshots/nested/snap_1.sql')
        self.assertIn(path, self.parser.results.files)
        self.assertEqual(self.parser.results.files[path].nodes, ['snapshot.snowplow.foo'])

    def test_multi_block(self):
        raw_1 = '''
            {{
                config(unique_key="id", target_schema="analytics",
                       target_database="dbt", strategy="timestamp",
                       updated_at="last_update")
            }}
            select 1 as id, now() as last_update
        '''
        raw_2 = '''
            {{
                config(unique_key="id", target_schema="analytics",
                       target_database="dbt", strategy="timestamp",
                       updated_at="last_update")
            }}
            select 2 as id, now() as last_update
        '''
        full_file = '''
        {{% snapshot foo %}}{}{{% endsnapshot %}}
        {{% snapshot bar %}}{}{{% endsnapshot %}}
        '''.format(raw_1, raw_2)
        block = self.file_block_for(full_file, 'nested/snap_1.sql')
        self.parser.parse_file(block)
        self.assert_has_results_length(self.parser.results, nodes=2)
        nodes = sorted(self.parser.results.nodes.values(), key=lambda n: n.name)
        expect_foo = ParsedSnapshotNode(
            alias='foo',
            name='foo',
            database='dbt',
            schema='analytics',
            resource_type=NodeType.Snapshot,
            unique_id='snapshot.snowplow.foo',
            fqn=['snowplow', 'nested', 'snap_1', 'foo'],
            package_name='snowplow',
            original_file_path=normalize('snapshots/nested/snap_1.sql'),
            root_path=get_abs_os_path('./dbt_modules/snowplow'),
            config=TimestampSnapshotConfig(
                strategy=SnapshotStrategy.Timestamp,
                updated_at='last_update',
                target_database='dbt',
                target_schema='analytics',
                unique_key='id',
                materialized='snapshot',
            ),
            path=normalize('nested/snap_1.sql'),
            raw_sql=raw_1,
        )
        expect_bar = ParsedSnapshotNode(
            alias='bar',
            name='bar',
            database='dbt',
            schema='analytics',
            resource_type=NodeType.Snapshot,
            unique_id='snapshot.snowplow.bar',
            fqn=['snowplow', 'nested', 'snap_1', 'bar'],
            package_name='snowplow',
            original_file_path=normalize('snapshots/nested/snap_1.sql'),
            root_path=get_abs_os_path('./dbt_modules/snowplow'),
            config=TimestampSnapshotConfig(
                strategy=SnapshotStrategy.Timestamp,
                updated_at='last_update',
                target_database='dbt',
                target_schema='analytics',
                unique_key='id',
                materialized='snapshot',
            ),
            path=normalize('nested/snap_1.sql'),
            raw_sql=raw_2,
        )
        self.assertEqual(nodes[0], expect_bar)
        self.assertEqual(nodes[1], expect_foo)
        path = get_abs_os_path('./dbt_modules/snowplow/snapshots/nested/snap_1.sql')
        self.assertIn(path, self.parser.results.files)
        self.assertEqual(sorted(self.parser.results.files[path].nodes),
                         ['snapshot.snowplow.bar', 'snapshot.snowplow.foo'])


class MacroParserTest(BaseParserTest):
    def setUp(self):
        super().setUp()
        self.parser = MacroParser(
            results=ParseResult.rpc(),
            project=self.snowplow_project_config,
        )

    def file_block_for(self, data, filename):
        return super().file_block_for(data, filename, 'macros')

    def test_single_block(self):
        raw_sql = '{% macro foo(a, b) %}a ~ b{% endmacro %}'
        block = self.file_block_for(raw_sql, 'macro.sql')
        self.parser.parse_file(block)
        self.assert_has_results_length(self.parser.results, macros=1)
        macro = list(self.parser.results.macros.values())[0]
        expected = ParsedMacro(
            name='foo',
            resource_type=NodeType.Macro,
            unique_id='macro.snowplow.foo',
            package_name='snowplow',
            original_file_path=normalize('macros/macro.sql'),
            root_path=get_abs_os_path('./dbt_modules/snowplow'),
            path=normalize('macros/macro.sql'),
            macro_sql=raw_sql,
        )
        self.assertEqual(macro, expected)
        path = get_abs_os_path('./dbt_modules/snowplow/macros/macro.sql')
        self.assertIn(path, self.parser.results.files)
        self.assertEqual(self.parser.results.files[path].macros, ['macro.snowplow.foo'])

    def test_multiple_blocks(self):
        raw_sql = '{% macro foo(a, b) %}a ~ b{% endmacro %}\n{% macro bar(c, d) %}c + d{% endmacro %}'
        block = self.file_block_for(raw_sql, 'macro.sql')
        self.parser.parse_file(block)
        self.assert_has_results_length(self.parser.results, macros=2)
        macros = sorted(self.parser.results.macros.values(), key=lambda m: m.name)
        expected_bar = ParsedMacro(
            name='bar',
            resource_type=NodeType.Macro,
            unique_id='macro.snowplow.bar',
            package_name='snowplow',
            original_file_path=normalize('macros/macro.sql'),
            root_path=get_abs_os_path('./dbt_modules/snowplow'),
            path=normalize('macros/macro.sql'),
            macro_sql='{% macro bar(c, d) %}c + d{% endmacro %}',
        )
        expected_foo = ParsedMacro(
            name='foo',
            resource_type=NodeType.Macro,
            unique_id='macro.snowplow.foo',
            package_name='snowplow',
            original_file_path=normalize('macros/macro.sql'),
            root_path=get_abs_os_path('./dbt_modules/snowplow'),
            path=normalize('macros/macro.sql'),
            macro_sql='{% macro foo(a, b) %}a ~ b{% endmacro %}',
        )
        self.assertEqual(macros, [expected_bar, expected_foo])
        path = get_abs_os_path('./dbt_modules/snowplow/macros/macro.sql')
        self.assertIn(path, self.parser.results.files)
        self.assertEqual(
            sorted(self.parser.results.files[path].macros),
            ['macro.snowplow.bar', 'macro.snowplow.foo'],
        )


class DataTestParserTest(BaseParserTest):
    def setUp(self):
        super().setUp()
        self.parser = DataTestParser(
            results=ParseResult.rpc(),
            project=self.snowplow_project_config,
            root_project=self.root_project_config,
            macro_manifest=self.macro_manifest,
        )

    def file_block_for(self, data, filename):
        return super().file_block_for(data, filename, 'tests')

    def test_basic(self):
        raw_sql = 'select * from {{ ref("blah") }} limit 0'
        block = self.file_block_for(raw_sql, 'test_1.sql')
        self.parser.parse_file(block)
        self.assert_has_results_length(self.parser.results, nodes=1)
        node = list(self.parser.results.nodes.values())[0]
        expected = ParsedDataTestNode(
            alias='test_1',
            name='test_1',
            database='test',
            schema='analytics',
            resource_type=NodeType.Test,
            unique_id='test.snowplow.test_1',
            fqn=['snowplow', 'data_test', 'test_1'],
            package_name='snowplow',
            original_file_path=normalize('tests/test_1.sql'),
            root_path=get_abs_os_path('./dbt_modules/snowplow'),
            refs=[['blah']],
            config=TestConfig(severity='ERROR'),
            tags=['data'],
            path=normalize('data_test/test_1.sql'),
            raw_sql=raw_sql,
        )
        self.assertEqual(node, expected)
        path = get_abs_os_path('./dbt_modules/snowplow/tests/test_1.sql')
        self.assertIn(path, self.parser.results.files)
        self.assertEqual(self.parser.results.files[path].nodes, ['test.snowplow.test_1'])


class AnalysisParserTest(BaseParserTest):
    def setUp(self):
        super().setUp()
        self.parser = AnalysisParser(
            results=ParseResult.rpc(),
            project=self.snowplow_project_config,
            root_project=self.root_project_config,
            macro_manifest=self.macro_manifest,
        )

    def file_block_for(self, data, filename):
        return super().file_block_for(data, filename, 'analyses')

    def test_basic(self):
        raw_sql = 'select 1 as id'
        block = self.file_block_for(raw_sql, 'nested/analysis_1.sql')
        self.parser.parse_file(block)
        self.assert_has_results_length(self.parser.results, nodes=1)
        node = list(self.parser.results.nodes.values())[0]
        expected = ParsedAnalysisNode(
            alias='analysis_1',
            name='analysis_1',
            database='test',
            schema='analytics',
            resource_type=NodeType.Analysis,
            unique_id='analysis.snowplow.analysis_1',
            fqn=['snowplow', 'analysis', 'nested', 'analysis_1'],
            package_name='snowplow',
            original_file_path=normalize('analyses/nested/analysis_1.sql'),
            root_path=get_abs_os_path('./dbt_modules/snowplow'),
            depends_on=DependsOn(),
            config=NodeConfig(),
            path=normalize('analysis/nested/analysis_1.sql'),
            raw_sql=raw_sql,
        )
        self.assertEqual(node, expected)
        path = get_abs_os_path('./dbt_modules/snowplow/analyses/nested/analysis_1.sql')
        self.assertIn(path, self.parser.results.files)
        self.assertEqual(self.parser.results.files[path].nodes, ['analysis.snowplow.analysis_1'])


class ProcessingTest(BaseParserTest):
    def setUp(self):
        super().setUp()
        x_depends_on = mock.MagicMock()
        y_depends_on = mock.MagicMock()
        self.x_node = MockNode(
            package='project',
            name='x',
            config=mock.MagicMock(enabled=True),
            refs=[],
            sources=[['src', 'tbl']],
            depends_on=x_depends_on,
            description='other_project: {{ doc("otherproject", "my_doc") }}',
        )
        self.y_node = MockNode(
            package='otherproject',
            name='y',
            config=mock.MagicMock(enabled=True),
            refs=[['x']],
            sources=[],
            depends_on=y_depends_on,
            description='{{ doc("my_doc") }}',
        )
        self.src_node = MockSource(
            package='thirdproject',
            source_name='src',
            name='tbl',
            config=mock.MagicMock(enabled=True),
        )
        self.doc = MockDocumentation(
            package='otherproject',
            name='my_doc',
            block_contents='some docs',
        )
        nodes = {
            self.x_node.unique_id: self.x_node,
            self.y_node.unique_id: self.y_node,
        }
        sources = {
            self.src_node.unique_id: self.src_node,
        }
        docs = {
            self.doc.unique_id: self.doc,
        }
        self.manifest = Manifest(
            nodes=nodes, sources=sources, macros={}, docs=docs, disabled=[], files={}, generated_at=mock.MagicMock()
        )

    def test_process_docs(self):
        process_docs(self.manifest, self.root_project_config)
        self.assertEqual(self.x_node.description, 'other_project: some docs')
        self.assertEqual(self.y_node.description, 'some docs')

    def test_process_sources(self):
        process_sources(self.manifest, 'project')
        self.x_node.depends_on.nodes.append.assert_called_once_with('source.thirdproject.src.tbl')

    def test_process_refs(self):
        process_refs(self.manifest, 'project')
        self.y_node.depends_on.nodes.append.assert_called_once_with('model.project.x')
