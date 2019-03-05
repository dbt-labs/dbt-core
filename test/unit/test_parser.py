import unittest
import mock

import os
import yaml

import dbt.flags
import dbt.parser
from dbt.parser import ModelParser, MacroParser, DataTestParser, SchemaParser, ParserUtils
from dbt.parser.source_config import SourceConfig
from dbt.utils import timestring
from dbt.config import RuntimeConfig

from dbt.node_types import NodeType
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import ParsedNode, ParsedMacro, \
    ParsedNodePatch, ParsedSourceDefinition
from dbt.contracts.graph.unparsed import UnparsedNode

from .utils import config_from_parts_or_dicts

def get_os_path(unix_path):
    return os.path.normpath(unix_path)


class BaseParserTest(unittest.TestCase):

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
            'project-root': os.path.abspath('.'),
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
            'project-root': os.path.abspath('./dbt_modules/snowplow'),
        }

        self.snowplow_project_config = config_from_parts_or_dicts(
            project=snowplow_project, profile=profile_data
        )

        self.all_projects = {
            'root': self.root_project_config,
            'snowplow': self.snowplow_project_config
        }



class SourceConfigTest(BaseParserTest):
    def test__source_config_single_call(self):
        cfg = SourceConfig(self.root_project_config, self.root_project_config,
                           ['root', 'x'], NodeType.Model)
        cfg.update_in_model_config({
            'materialized': 'something',
            'sort': 'my sort key',
            'pre-hook': 'my pre run hook',
            'vars': {'a': 1, 'b': 2},
        })
        expect = {
            'column_types': {},
            'enabled': True,
            'materialized': 'something',
            'post-hook': [],
            'pre-hook': ['my pre run hook'],
            'quoting': {},
            'sort': 'my sort key',
            'tags': [],
            'vars': {'a': 1, 'b': 2},
        }
        self.assertEqual(cfg.config, expect)

    def test__source_config_multiple_calls(self):
        cfg = SourceConfig(self.root_project_config, self.root_project_config,
                           ['root', 'x'], NodeType.Model)
        cfg.update_in_model_config({
            'materialized': 'something',
            'sort': 'my sort key',
            'pre-hook': 'my pre run hook',
            'vars': {'a': 1, 'b': 2},
        })
        cfg.update_in_model_config({
            'materialized': 'something else',
            'pre-hook': ['my other pre run hook', 'another pre run hook'],
            'vars': {'a': 4, 'c': 3},
        })
        expect = {
            'column_types': {},
            'enabled': True,
            'materialized': 'something else',
            'post-hook': [],
            'pre-hook': [
                'my pre run hook',
                'my other pre run hook',
                'another pre run hook',
            ],
            'quoting': {},
            'sort': 'my sort key',
            'tags': [],
            'vars': {'a': 4, 'b': 2, 'c': 3},
        }
        self.assertEqual(cfg.config, expect)

    def test_source_config_all_keys_accounted_for(self):
        used_keys = frozenset(SourceConfig.AppendListFields) | \
                    frozenset(SourceConfig.ExtendDictFields) | \
                    frozenset(SourceConfig.ClobberFields)

        self.assertEqual(used_keys, frozenset(SourceConfig.ConfigKeys))


class SchemaParserTest(BaseParserTest):
    maxDiff = None

    def setUp(self):
        super(SchemaParserTest, self).setUp()
        self.maxDiff = None


        self.macro_manifest = Manifest(macros={}, nodes={}, docs={},
                                       generated_at=timestring(), disabled=[])

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

        self.disabled_config = {
            'enabled': False,
            'materialized': 'view',
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
            'quoting': {},
            'column_types': {},
            'tags': [],
        }

        self._expected_source = ParsedSourceDefinition(
            unique_id='source.root.my_source.my_table',
            name='my_table',
            description='my table description',
            source_name='my_source',
            source_description='my source description',
            loader='some_loader',
            package_name='root',
            root_path=get_os_path('/usr/src/app'),
            path='test_one.yml',
            original_file_path='test_one.yml',
            columns={
                'id': {
                    'name': 'id',
                    'description': 'user ID',
                },
            },
            docrefs=[],
            freshness={
                'warn_after': {
                    'count': 7,
                    'period': 'hour'
                },
                'error_after': {
                    'count': 20,
                    'period': 'hour'
                },
            },
            loaded_at_field='something',
            database='test',
            schema='foo',
            identifier='bar',
            resource_type='source',
            quoting={
                'schema': True,
                'identifier': False,
            }
        )

        self._expected_source_tests = [
            ParsedNode(
                alias='source_accepted_values_my_source_my_table_id__a__b',
                name='source_accepted_values_my_source_my_table_id__a__b',
                database='test',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.source_accepted_values_my_source_my_table_id__a__b',
                fqn=['root', 'schema_test',
                        'source_accepted_values_my_source_my_table_id__a__b'],
                empty=False,
                package_name='root',
                original_file_path='test_one.yml',
                root_path=get_os_path('/usr/src/app'),
                refs=[],
                sources=[['my_source', 'my_table']],
                depends_on={'nodes': [], 'macros': []},
                config=self.model_config,
                path=get_os_path(
                    'schema_test/source_accepted_values_my_source_my_table_id__a__b.sql'),
                tags=['schema'],
                raw_sql="{{ test_accepted_values(model=source('my_source', 'my_table'), column_name='id', values=['a', 'b']) }}",
                description='',
                columns={},
                column_name='id'
            ),
            ParsedNode(
                alias='source_not_null_my_source_my_table_id',
                name='source_not_null_my_source_my_table_id',
                database='test',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.source_not_null_my_source_my_table_id',
                fqn=['root', 'schema_test', 'source_not_null_my_source_my_table_id'],
                empty=False,
                package_name='root',
                root_path=get_os_path('/usr/src/app'),
                refs=[],
                sources=[['my_source', 'my_table']],
                depends_on={'nodes': [], 'macros': []},
                config=self.model_config,
                original_file_path='test_one.yml',
                path=get_os_path('schema_test/source_not_null_my_source_my_table_id.sql'),
                tags=['schema'],
                raw_sql="{{ test_not_null(model=source('my_source', 'my_table'), column_name='id') }}",
                description='',
                columns={},
                column_name='id'
            ),
            ParsedNode(
                alias='source_relationships_my_source_my_table_id__id__ref_model_two_',
                name='source_relationships_my_source_my_table_id__id__ref_model_two_',
                database='test',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.source_relationships_my_source_my_table_id__id__ref_model_two_', # noqa
                fqn=['root', 'schema_test',
                        'source_relationships_my_source_my_table_id__id__ref_model_two_'],
                empty=False,
                package_name='root',
                original_file_path='test_one.yml',
                root_path=get_os_path('/usr/src/app'),
                refs=[['model_two']],
                sources=[['my_source', 'my_table']],
                depends_on={'nodes': [], 'macros': []},
                config=self.model_config,
                path=get_os_path('schema_test/source_relationships_my_source_my_table_id__id__ref_model_two_.sql'), # noqa
                tags=['schema'],
                raw_sql="{{ test_relationships(model=source('my_source', 'my_table'), column_name='id', from='id', to=ref('model_two')) }}",
                description='',
                columns={},
                column_name='id'
            ),
            ParsedNode(
                alias='source_some_test_my_source_my_table_value',
                name='source_some_test_my_source_my_table_value',
                database='test',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.source_some_test_my_source_my_table_value',
                fqn=['root', 'schema_test', 'source_some_test_my_source_my_table_value'],
                empty=False,
                package_name='root',
                original_file_path='test_one.yml',
                root_path=get_os_path('/usr/src/app'),
                refs=[],
                sources=[['my_source', 'my_table']],
                depends_on={'nodes': [], 'macros': []},
                config=self.model_config,
                path=get_os_path('schema_test/source_some_test_my_source_my_table_value.sql'),
                tags=['schema'],
                raw_sql="{{ test_some_test(model=source('my_source', 'my_table'), key='value') }}",
                description='',
                columns={}
            ),
            ParsedNode(
                alias='source_unique_my_source_my_table_id',
                name='source_unique_my_source_my_table_id',
                database='test',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.source_unique_my_source_my_table_id',
                fqn=['root', 'schema_test', 'source_unique_my_source_my_table_id'],
                empty=False,
                package_name='root',
                root_path=get_os_path('/usr/src/app'),
                refs=[],
                sources=[['my_source', 'my_table']],
                depends_on={'nodes': [], 'macros': []},
                config=self.model_config,
                original_file_path='test_one.yml',
                path=get_os_path('schema_test/source_unique_my_source_my_table_id.sql'),
                tags=['schema'],
                raw_sql="{{ test_unique(model=source('my_source', 'my_table'), column_name='id') }}",
                description='',
                columns={},
                column_name='id'
            ),
        ]

        self._expected_model_tests = [
            ParsedNode(
                alias='accepted_values_model_one_id__a__b',
                name='accepted_values_model_one_id__a__b',
                database='test',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.accepted_values_model_one_id__a__b',
                fqn=['root', 'schema_test',
                        'accepted_values_model_one_id__a__b'],
                empty=False,
                package_name='root',
                original_file_path='test_one.yml',
                root_path=get_os_path('/usr/src/app'),
                refs=[['model_one']],
                sources=[],
                depends_on={'nodes': [], 'macros': []},
                config=self.model_config,
                path=get_os_path(
                    'schema_test/accepted_values_model_one_id__a__b.sql'),
                tags=['schema'],
                raw_sql="{{ test_accepted_values(model=ref('model_one'), column_name='id', values=['a', 'b']) }}",
                description='',
                columns={},
                column_name='id'
            ),
            ParsedNode(
                alias='not_null_model_one_id',
                name='not_null_model_one_id',
                database='test',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.not_null_model_one_id',
                fqn=['root', 'schema_test', 'not_null_model_one_id'],
                empty=False,
                package_name='root',
                root_path=get_os_path('/usr/src/app'),
                refs=[['model_one']],
                sources=[],
                depends_on={'nodes': [], 'macros': []},
                config=self.model_config,
                original_file_path='test_one.yml',
                path=get_os_path('schema_test/not_null_model_one_id.sql'),
                tags=['schema'],
                raw_sql="{{ test_not_null(model=ref('model_one'), column_name='id') }}",
                description='',
                columns={},
                column_name='id'
            ),
            ParsedNode(
                alias='relationships_model_one_id__id__ref_model_two_',
                name='relationships_model_one_id__id__ref_model_two_',
                database='test',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.relationships_model_one_id__id__ref_model_two_', # noqa
                fqn=['root', 'schema_test',
                        'relationships_model_one_id__id__ref_model_two_'],
                empty=False,
                package_name='root',
                original_file_path='test_one.yml',
                root_path=get_os_path('/usr/src/app'),
                refs=[['model_one'], ['model_two']],
                sources=[],
                depends_on={'nodes': [], 'macros': []},
                config=self.model_config,
                path=get_os_path('schema_test/relationships_model_one_id__id__ref_model_two_.sql'), # noqa
                tags=['schema'],
                raw_sql="{{ test_relationships(model=ref('model_one'), column_name='id', from='id', to=ref('model_two')) }}",
                description='',
                columns={},
                column_name='id'
            ),
            ParsedNode(
                alias='some_test_model_one_value',
                name='some_test_model_one_value',
                database='test',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.some_test_model_one_value',
                fqn=['root', 'schema_test', 'some_test_model_one_value'],
                empty=False,
                package_name='root',
                original_file_path='test_one.yml',
                root_path=get_os_path('/usr/src/app'),
                refs=[['model_one']],
                sources=[],
                depends_on={'nodes': [], 'macros': []},
                config=self.model_config,
                path=get_os_path('schema_test/some_test_model_one_value.sql'),
                tags=['schema'],
                raw_sql="{{ test_some_test(model=ref('model_one'), key='value') }}",
                description='',
                columns={}
            ),
            ParsedNode(
                alias='unique_model_one_id',
                name='unique_model_one_id',
                database='test',
                schema='analytics',
                resource_type='test',
                unique_id='test.root.unique_model_one_id',
                fqn=['root', 'schema_test', 'unique_model_one_id'],
                empty=False,
                package_name='root',
                root_path=get_os_path('/usr/src/app'),
                refs=[['model_one']],
                sources=[],
                depends_on={'nodes': [], 'macros': []},
                config=self.model_config,
                original_file_path='test_one.yml',
                path=get_os_path('schema_test/unique_model_one_id.sql'),
                tags=['schema'],
                raw_sql="{{ test_unique(model=ref('model_one'), column_name='id') }}",
                description='',
                columns={},
                column_name='id'
            ),
        ]

        self._expected_patch = ParsedNodePatch(
            name='model_one',
            description='blah blah',
            original_file_path='test_one.yml',
            columns={
                'id': {
                'name': 'id',
                'description': 'user ID',
            }},
            docrefs=[],
        )

    def test__source_schema(self):
        test_yml = yaml.safe_load('''
            version: 2
            sources:
                - name: my_source
                  loader: some_loader
                  description: my source description
                  quoting:
                    schema: True
                    identifier: True
                  freshness:
                    warn_after:
                        count: 10
                        period: hour
                    error_after:
                        count: 20
                        period: hour
                  loaded_at_field: something
                  schema: '{{ var("test_schema_name") }}'
                  tables:
                    - name: my_table
                      description: "my table description"
                      identifier: bar
                      freshness:
                        warn_after:
                            count: 7
                            period: hour
                      quoting:
                        identifier: False
                      columns:
                        - name: id
                          description: user ID
                          tests:
                            - unique
                            - not_null
                            - accepted_values:
                                values:
                                  - a
                                  - b
                            - relationships:
                                from: id
                                to: ref('model_two')
                      tests:
                        - some_test:
                            key: value
        ''')
        parser = SchemaParser(
            self.root_project_config,
            self.all_projects,
            self.macro_manifest
        )
        root_dir = get_os_path('/usr/src/app')
        results = list(parser.parse_schema(
            path='test_one.yml',
            test_yml=test_yml,
            package_name='root',
            root_dir=root_dir
        ))

        tests = sorted((node for t, node in results if t == 'test'),
                       key=lambda n: n.name)
        patches = sorted((node for t, node in results if t == 'patch'),
                         key=lambda n: n.name)
        sources = sorted((node for t, node in results if t == 'source'),
                         key=lambda n: n.name)
        self.assertEqual(len(tests), 5)
        self.assertEqual(len(patches), 0)
        self.assertEqual(len(sources), 1)
        self.assertEqual(len(results), 6)

        for test, expected in zip(tests, self._expected_source_tests):
            self.assertEqual(test, expected)

        self.assertEqual(sources[0], self._expected_source)

    def test__model_schema(self):
        test_yml = yaml.safe_load('''
            version: 2
            models:
                - name: model_one
                  description: blah blah
                  columns:
                    - name: id
                      description: user ID
                      tests:
                        - unique
                        - not_null
                        - accepted_values:
                            values:
                              - a
                              - b
                        - relationships:
                            from: id
                            to: ref('model_two')
                  tests:
                    - some_test:
                        key: value
        ''')
        parser = SchemaParser(
            self.root_project_config,
            self.all_projects,
            self.macro_manifest
        )
        results = list(parser.parse_schema(
            path='test_one.yml',
            test_yml=test_yml,
            package_name='root',
            root_dir=get_os_path('/usr/src/app')
        ))

        tests = sorted((node for t, node in results if t == 'test'),
                       key=lambda n: n.name)
        patches = sorted((node for t, node in results if t == 'patch'),
                         key=lambda n: n.name)
        sources = sorted((node for t, node in results if t == 'source'),
                         key=lambda n: n.name)
        self.assertEqual(len(tests), 5)
        self.assertEqual(len(patches), 1)
        self.assertEqual(len(sources), 0)
        self.assertEqual(len(results), 6)

        for test, expected in zip(tests, self._expected_model_tests):
            self.assertEqual(test, expected)


        self.assertEqual(patches[0], self._expected_patch)

    def test__mixed_schema(self):
        test_yml = yaml.safe_load('''
            version: 2
            quoting:
              database: True
            models:
                - name: model_one
                  description: blah blah
                  columns:
                    - name: id
                      description: user ID
                      tests:
                        - unique
                        - not_null
                        - accepted_values:
                            values:
                              - a
                              - b
                        - relationships:
                            from: id
                            to: ref('model_two')
                  tests:
                    - some_test:
                        key: value
            sources:
                - name: my_source
                  loader: some_loader
                  description: my source description
                  quoting:
                    schema: True
                    identifier: True
                  freshness:
                    warn_after:
                        count: 10
                        period: hour
                    error_after:
                        count: 20
                        period: hour
                  loaded_at_field: something
                  schema: '{{ var("test_schema_name") }}'
                  tables:
                    - name: my_table
                      description: "my table description"
                      identifier: bar
                      freshness:
                        warn_after:
                            count: 7
                            period: hour
                      quoting:
                        identifier: False
                      columns:
                        - name: id
                          description: user ID
                          tests:
                            - unique
                            - not_null
                            - accepted_values:
                                values:
                                  - a
                                  - b
                            - relationships:
                                from: id
                                to: ref('model_two')
                      tests:
                        - some_test:
                            key: value
        ''')
        parser = SchemaParser(
            self.root_project_config,
            self.all_projects,
            self.macro_manifest
        )
        results = list(parser.parse_schema(
            path='test_one.yml',
            test_yml=test_yml,
            package_name='root',
            root_dir=get_os_path('/usr/src/app')
        ))

        tests = sorted((node for t, node in results if t == 'test'),
                       key=lambda n: n.name)
        patches = sorted((node for t, node in results if t == 'patch'),
                         key=lambda n: n.name)
        sources = sorted((node for t, node in results if t == 'source'),
                         key=lambda n: n.name)
        self.assertEqual(len(tests), 10)
        self.assertEqual(len(patches), 1)
        self.assertEqual(len(sources), 1)
        self.assertEqual(len(results), 12)

        expected_tests = self._expected_model_tests + self._expected_source_tests
        expected_tests.sort(key=lambda n: n.name)
        for test, expected in zip(tests, expected_tests):
            self.assertEqual(test, expected)

        self.assertEqual(patches[0], self._expected_patch)
        self.assertEqual(sources[0], self._expected_source)

    def test__source_schema_invalid_test_strict(self):
        test_yml = yaml.safe_load('''
            version: 2
            sources:
                - name: my_source
                  loader: some_loader
                  description: my source description
                  quoting:
                    schema: True
                    identifier: True
                  freshness:
                    warn_after:
                        count: 10
                        period: hour
                    error_after:
                        count: 20
                        period: hour
                  loaded_at_field: something
                  schema: foo
                  tables:
                    - name: my_table
                      description: "my table description"
                      identifier: bar
                      freshness:
                        warn_after:
                            count: 7
                            period: hour
                      quoting:
                        identifier: False
                      columns:
                        - name: id
                          description: user ID
                          tests:
                            - unique
                            - not_null
                            - accepted_values: # this test is invalid
                                - values:
                                    - a
                                    - b
                            - relationships:
                                from: id
                                to: ref('model_two')
                      tests:
                        - some_test:
                            key: value
        ''')
        parser = SchemaParser(
            self.root_project_config,
            self.all_projects,
            self.macro_manifest
        )
        root_dir = get_os_path('/usr/src/app')
        with self.assertRaises(dbt.exceptions.CompilationException):
            list(parser.parse_schema(
                path='test_one.yml',
                test_yml=test_yml,
                package_name='root',
                root_dir=root_dir
            ))

    def test__source_schema_invalid_test_not_strict(self):
        dbt.flags.WARN_ERROR = False
        dbt.flags.STRICT_MODE = False
        test_yml = yaml.safe_load('''
            version: 2
            sources:
                - name: my_source
                  loader: some_loader
                  description: my source description
                  quoting:
                    schema: True
                    identifier: True
                  freshness:
                    warn_after:
                        count: 10
                        period: hour
                    error_after:
                        count: 20
                        period: hour
                  loaded_at_field: something
                  schema: foo
                  tables:
                    - name: my_table
                      description: "my table description"
                      identifier: bar
                      freshness:
                        warn_after:
                            count: 7
                            period: hour
                      quoting:
                        identifier: False
                      columns:
                        - name: id
                          description: user ID
                          tests:
                            - unique
                            - not_null
                            - accepted_values: # this test is invalid
                                - values:
                                    - a
                                    - b
                            - relationships:
                                from: id
                                to: ref('model_two')
                      tests:
                        - some_test:
                            key: value
        ''')
        parser = SchemaParser(
            self.root_project_config,
            self.all_projects,
            self.macro_manifest
        )
        root_dir = get_os_path('/usr/src/app')
        results = list(parser.parse_schema(
            path='test_one.yml',
            test_yml=test_yml,
            package_name='root',
            root_dir=root_dir
        ))

        tests = sorted((node for t, node in results if t == 'test'),
                       key=lambda n: n.name)
        patches = sorted((node for t, node in results if t == 'patch'),
                         key=lambda n: n.name)
        sources = sorted((node for t, node in results if t == 'source'),
                         key=lambda n: n.name)
        self.assertEqual(len(tests), 4)
        self.assertEqual(len(patches), 0)
        self.assertEqual(len(sources), 1)
        self.assertEqual(len(results), 5)

        expected_tests = [x for x in self._expected_source_tests
                          if 'accepted_values' not in x.unique_id]
        for test, expected in zip(tests, expected_tests):
            self.assertEqual(test, expected)

        self.assertEqual(sources[0], self._expected_source)

    @mock.patch.object(SchemaParser, 'find_schema_yml')
    @mock.patch.object(dbt.parser.schemas, 'logger')
    def test__schema_v2_as_v1(self, mock_logger, find_schema_yml):
        test_yml = yaml.safe_load(
            '{models: [{name: model_one, description: "blah blah", columns: ['
            '{name: id, description: "user ID", tests: [unique, not_null, '
            '{accepted_values: {values: ["a", "b"]}},'
            '{relationships: {from: id, to: ref(\'model_two\')}}]'
            '}], tests: [some_test: { key: value }]}]}'
        )
        find_schema_yml.return_value = [('/some/path/schema.yml', test_yml)]
        root_project = {}
        all_projects = {}
        root_dir = '/some/path'
        relative_dirs = ['a', 'b']
        parser = dbt.parser.schemas.SchemaParser(root_project, all_projects, None)
        with self.assertRaises(dbt.exceptions.CompilationException) as cm:
            parser.load_and_parse(
                'test', root_dir, relative_dirs
            )
            self.assertIn('https://docs.getdbt.com/v0.11/docs/schemayml-files',
                          str(cm.exception))

    @mock.patch.object(SchemaParser, 'find_schema_yml')
    @mock.patch.object(dbt.parser.schemas, 'logger')
    def test__schema_v1_version_model(self, mock_logger, find_schema_yml):
        test_yml = yaml.safe_load(
            '{model_one: {constraints: {not_null: [id],'
            'unique: [id],'
            'accepted_values: [{field: id, values: ["a","b"]}],'
            'relationships: [{from: id, to: ref(\'model_two\'), field: id}]' # noqa
            '}}, version: {constraints: {not_null: [id]}}}'
        )
        find_schema_yml.return_value = [('/some/path/schema.yml', test_yml)]
        root_project = {}
        all_projects = {}
        root_dir = '/some/path'
        relative_dirs = ['a', 'b']
        parser = dbt.parser.schemas.SchemaParser(root_project, all_projects, None)
        with self.assertRaises(dbt.exceptions.CompilationException) as cm:
            parser.load_and_parse(
                'test', root_dir, relative_dirs
            )
            self.assertIn('https://docs.getdbt.com/v0.11/docs/schemayml-files',
                          str(cm.exception))

    @mock.patch.object(SchemaParser, 'find_schema_yml')
    @mock.patch.object(dbt.parser.schemas, 'logger')
    def test__schema_v1_version_1(self, mock_logger, find_schema_yml):
        test_yml = yaml.safe_load(
            '{model_one: {constraints: {not_null: [id],'
            'unique: [id],'
            'accepted_values: [{field: id, values: ["a","b"]}],'
            'relationships: [{from: id, to: ref(\'model_two\'), field: id}]' # noqa
            '}}, version: 1}'
        )
        find_schema_yml.return_value = [('/some/path/schema.yml', test_yml)]
        root_project = {}
        all_projects = {}
        root_dir = '/some/path'
        relative_dirs = ['a', 'b']
        parser = dbt.parser.schemas.SchemaParser(root_project, all_projects, None)
        with self.assertRaises(dbt.exceptions.CompilationException) as cm:
            parser.load_and_parse(
                'test', root_dir, relative_dirs
            )
            self.assertIn('https://docs.getdbt.com/v0.11/docs/schemayml-files',
                          str(cm.exception))


class ParserTest(BaseParserTest):

    def find_input_by_name(self, models, name):
        return next(
            (model for model in models if model.get('name') == name),
            {})

    def setUp(self):
        super(ParserTest, self).setUp()

        self.macro_manifest = Manifest(macros={}, nodes={}, docs={},
                                       generated_at=timestring(), disabled=[])

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

        self.disabled_config = {
            'enabled': False,
            'materialized': 'view',
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
            'quoting': {},
            'column_types': {},
            'tags': [],
        }


    def test__single_model(self):
        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'original_file_path': 'model_one.sql',
            'root_path': get_os_path('/usr/src/app'),
            'path': 'model_one.sql',
            'raw_sql': ("select * from events"),
        }]
        parser = ModelParser(
            self.root_project_config,
            self.all_projects,
            self.macro_manifest
        )

        self.assertEqual(
            parser.parse_sql_nodes(models),
            ({
                'model.root.model_one': ParsedNode(
                    alias='model_one',
                    name='model_one',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.model_one',
                    fqn=['root', 'model_one'],
                    empty=False,
                    package_name='root',
                    original_file_path='model_one.sql',
                    root_path=get_os_path('/usr/src/app'),
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='model_one.sql',
                    raw_sql=self.find_input_by_name(
                        models, 'model_one').get('raw_sql'),
                    description='',
                    columns={}
                )
            }, [])
        )

    def test__single_model__nested_configuration(self):
        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'original_file_path': 'nested/path/model_one.sql',
            'root_path': get_os_path('/usr/src/app'),
            'path': get_os_path('nested/path/model_one.sql'),
            'raw_sql': ("select * from events"),
        }]

        self.root_project_config.models = {
            'materialized': 'ephemeral',
            'root': {
                'nested': {
                    'path': {
                        'materialized': 'ephemeral'
                    }
                }
            }
        }

        ephemeral_config = self.model_config.copy()
        ephemeral_config.update({
            'materialized': 'ephemeral'
        })

        parser = ModelParser(
            self.root_project_config,
            self.all_projects,
            self.macro_manifest
        )
        self.assertEqual(
            parser.parse_sql_nodes(models),
            ({
                'model.root.model_one': ParsedNode(
                    alias='model_one',
                    name='model_one',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.model_one',
                    fqn=['root', 'nested', 'path', 'model_one'],
                    empty=False,
                    package_name='root',
                    original_file_path='nested/path/model_one.sql',
                    root_path=get_os_path('/usr/src/app'),
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=ephemeral_config,
                    tags=[],
                    path=get_os_path('nested/path/model_one.sql'),
                    raw_sql=self.find_input_by_name(
                        models, 'model_one').get('raw_sql'),
                    description='',
                    columns={}
                )
            }, [])
        )

    def test__empty_model(self):
        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'model_one.sql',
            'original_file_path': 'model_one.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': (" "),
        }]

        del self.all_projects['snowplow']
        parser = ModelParser(
            self.root_project_config,
            self.all_projects,
            self.macro_manifest
        )

        self.assertEqual(
            parser.parse_sql_nodes(models),
            ({
                'model.root.model_one': ParsedNode(
                    alias='model_one',
                    name='model_one',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.model_one',
                    fqn=['root', 'model_one'],
                    empty=True,
                    package_name='root',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': [],
                    },
                    config=self.model_config,
                    tags=[],
                    path='model_one.sql',
                    original_file_path='model_one.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'model_one').get('raw_sql'),
                    description='',
                    columns={}
                )
            }, [])
        )

    def test__simple_dependency(self):
        models = [{
            'name': 'base',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'base.sql',
            'original_file_path': 'base.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': 'select * from events'
        }, {
            'name': 'events_tx',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'events_tx.sql',
            'original_file_path': 'events_tx.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': "select * from {{ref('base')}}"
        }]

        parser = ModelParser(
            self.root_project_config,
            self.all_projects,
            self.macro_manifest
        )

        self.assertEqual(
            parser.parse_sql_nodes(models),
            ({
                'model.root.base': ParsedNode(
                    alias='base',
                    name='base',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.base',
                    fqn=['root', 'base'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='base.sql',
                    original_file_path='base.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'base').get('raw_sql'),
                    description='',
                    columns={}

                ),
                'model.root.events_tx': ParsedNode(
                    alias='events_tx',
                    name='events_tx',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.events_tx',
                    fqn=['root', 'events_tx'],
                    empty=False,
                    package_name='root',
                    refs=[['base']],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='events_tx.sql',
                    original_file_path='events_tx.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'events_tx').get('raw_sql'),
                    description='',
                    columns={}
                )
            }, [])
        )

    def test__multiple_dependencies(self):
        models = [{
            'name': 'events',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'events.sql',
            'original_file_path': 'events.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': 'select * from base.events',
        }, {
            'name': 'sessions',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'sessions.sql',
            'original_file_path': 'sessions.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': 'select * from base.sessions',
        }, {
            'name': 'events_tx',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'events_tx.sql',
            'original_file_path': 'events_tx.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("with events as (select * from {{ref('events')}}) "
                        "select * from events"),
        }, {
            'name': 'sessions_tx',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'sessions_tx.sql',
            'original_file_path': 'sessions_tx.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("with sessions as (select * from {{ref('sessions')}}) "
                        "select * from sessions"),
        }, {
            'name': 'multi',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'multi.sql',
            'original_file_path': 'multi.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("with s as (select * from {{ref('sessions_tx')}}), "
                        "e as (select * from {{ref('events_tx')}}) "
                        "select * from e left join s on s.id = e.sid"),
        }]

        parser = ModelParser(
            self.root_project_config,
            self.all_projects,
            self.macro_manifest
        )

        self.assertEqual(
            parser.parse_sql_nodes(models),
            ({
                'model.root.events': ParsedNode(
                    alias='events',
                    name='events',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.events',
                    fqn=['root', 'events'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='events.sql',
                    original_file_path='events.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'events').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.root.sessions': ParsedNode(
                    alias='sessions',
                    name='sessions',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.sessions',
                    fqn=['root', 'sessions'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='sessions.sql',
                    original_file_path='sessions.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'sessions').get('raw_sql'),
                    description='',
                    columns={},
                ),
                'model.root.events_tx': ParsedNode(
                    alias='events_tx',
                    name='events_tx',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.events_tx',
                    fqn=['root', 'events_tx'],
                    empty=False,
                    package_name='root',
                    refs=[['events']],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='events_tx.sql',
                    original_file_path='events_tx.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'events_tx').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.root.sessions_tx': ParsedNode(
                    alias='sessions_tx',
                    name='sessions_tx',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.sessions_tx',
                    fqn=['root', 'sessions_tx'],
                    empty=False,
                    package_name='root',
                    refs=[['sessions']],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='sessions_tx.sql',
                    original_file_path='sessions_tx.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'sessions_tx').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.root.multi': ParsedNode(
                    alias='multi',
                    name='multi',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.multi',
                    fqn=['root', 'multi'],
                    empty=False,
                    package_name='root',
                    refs=[['sessions_tx'], ['events_tx']],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='multi.sql',
                    original_file_path='multi.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'multi').get('raw_sql'),
                    description='',
                    columns={}
                ),
            }, [])
        )

    def test__multiple_dependencies__packages(self):
        models = [{
            'name': 'events',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'events.sql',
            'original_file_path': 'events.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': 'select * from base.events',
        }, {
            'name': 'sessions',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'sessions.sql',
            'original_file_path': 'sessions.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': 'select * from base.sessions',
        }, {
            'name': 'events_tx',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'events_tx.sql',
            'original_file_path': 'events_tx.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("with events as (select * from {{ref('events')}}) "
                        "select * from events"),
        }, {
            'name': 'sessions_tx',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'sessions_tx.sql',
            'original_file_path': 'sessions_tx.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("with sessions as (select * from {{ref('sessions')}}) "
                        "select * from sessions"),
        }, {
            'name': 'multi',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'multi.sql',
            'original_file_path': 'multi.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("with s as "
                        "(select * from {{ref('snowplow', 'sessions_tx')}}), "
                        "e as "
                        "(select * from {{ref('snowplow', 'events_tx')}}) "
                        "select * from e left join s on s.id = e.sid"),
        }]

        parser = ModelParser(
            self.root_project_config,
            self.all_projects,
            self.macro_manifest
        )

        self.assertEqual(
            parser.parse_sql_nodes(models),
            ({
                'model.snowplow.events': ParsedNode(
                    alias='events',
                    name='events',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.snowplow.events',
                    fqn=['snowplow', 'events'],
                    empty=False,
                    package_name='snowplow',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='events.sql',
                    original_file_path='events.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'events').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.snowplow.sessions': ParsedNode(
                    alias='sessions',
                    name='sessions',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.snowplow.sessions',
                    fqn=['snowplow', 'sessions'],
                    empty=False,
                    package_name='snowplow',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='sessions.sql',
                    original_file_path='sessions.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'sessions').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.snowplow.events_tx': ParsedNode(
                    alias='events_tx',
                    name='events_tx',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.snowplow.events_tx',
                    fqn=['snowplow', 'events_tx'],
                    empty=False,
                    package_name='snowplow',
                    refs=[['events']],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='events_tx.sql',
                    original_file_path='events_tx.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'events_tx').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.snowplow.sessions_tx': ParsedNode(
                    alias='sessions_tx',
                    name='sessions_tx',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.snowplow.sessions_tx',
                    fqn=['snowplow', 'sessions_tx'],
                    empty=False,
                    package_name='snowplow',
                    refs=[['sessions']],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='sessions_tx.sql',
                    original_file_path='sessions_tx.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'sessions_tx').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.root.multi': ParsedNode(
                    alias='multi',
                    name='multi',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.multi',
                    fqn=['root', 'multi'],
                    empty=False,
                    package_name='root',
                    refs=[['snowplow', 'sessions_tx'],
                             ['snowplow', 'events_tx']],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='multi.sql',
                    original_file_path='multi.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'multi').get('raw_sql'),
                    description='',
                    columns={}
                ),
            }, [])
        )

    def test__process_refs__packages(self):
        graph = {
            'macros': {},
            'nodes': {
                'model.snowplow.events': {
                    'name': 'events',
                    'alias': 'events',
                    'database': 'test',
                    'schema': 'analytics',
                    'resource_type': 'model',
                    'unique_id': 'model.snowplow.events',
                    'fqn': ['snowplow', 'events'],
                    'empty': False,
                    'package_name': 'snowplow',
                    'refs': [],
                    'sources': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.disabled_config,
                    'tags': [],
                    'path': 'events.sql',
                    'original_file_path': 'events.sql',
                    'root_path': get_os_path('/usr/src/app'),
                    'raw_sql': 'does not matter'
                },
                'model.root.events': {
                    'name': 'events',
                    'alias': 'events',
                    'database': 'test',
                    'schema': 'analytics',
                    'resource_type': 'model',
                    'unique_id': 'model.root.events',
                    'fqn': ['root', 'events'],
                    'empty': False,
                    'package_name': 'root',
                    'refs': [],
                    'sources': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'tags': [],
                    'path': 'events.sql',
                    'original_file_path': 'events.sql',
                    'root_path': get_os_path('/usr/src/app'),
                    'raw_sql': 'does not matter'
                },
                'model.root.dep': {
                    'name': 'dep',
                    'alias': 'dep',
                    'database': 'test',
                    'schema': 'analytics',
                    'resource_type': 'model',
                    'unique_id': 'model.root.dep',
                    'fqn': ['root', 'dep'],
                    'empty': False,
                    'package_name': 'root',
                    'refs': [['events']],
                    'sources': [],
                    'depends_on': {
                        'nodes': [],
                        'macros': []
                    },
                    'config': self.model_config,
                    'tags': [],
                    'path': 'multi.sql',
                    'original_file_path': 'multi.sql',
                    'root_path': get_os_path('/usr/src/app'),
                    'raw_sql': 'does not matter'
                }
            }
        }

        manifest = Manifest(
            nodes={k: ParsedNode(**v) for (k,v) in graph['nodes'].items()},
            macros={k: ParsedMacro(**v) for (k,v) in graph['macros'].items()},
            docs={},
            generated_at=timestring(),
            disabled=[]
        )

        processed_manifest = ParserUtils.process_refs(manifest, 'root')
        self.assertEqual(
            processed_manifest.to_flat_graph(),
            {
                'macros': {},
                'nodes': {
                    'model.snowplow.events': {
                        'name': 'events',
                        'alias': 'events',
                        'database': 'test',
                        'schema': 'analytics',
                        'resource_type': 'model',
                        'unique_id': 'model.snowplow.events',
                        'fqn': ['snowplow', 'events'],
                        'empty': False,
                        'package_name': 'snowplow',
                        'refs': [],
                        'sources': [],
                        'depends_on': {
                            'nodes': [],
                            'macros': []
                        },
                        'config': self.disabled_config,
                        'tags': [],
                        'path': 'events.sql',
                        'original_file_path': 'events.sql',
                        'root_path': get_os_path('/usr/src/app'),
                        'raw_sql': 'does not matter',
                        'agate_table': None,
                        'columns': {},
                        'description': '',
                    },
                    'model.root.events': {
                        'name': 'events',
                        'alias': 'events',
                        'database': 'test',
                        'schema': 'analytics',
                        'resource_type': 'model',
                        'unique_id': 'model.root.events',
                        'fqn': ['root', 'events'],
                        'empty': False,
                        'package_name': 'root',
                        'refs': [],
                        'sources': [],
                        'depends_on': {
                            'nodes': [],
                            'macros': []
                        },
                        'config': self.model_config,
                        'tags': [],
                        'path': 'events.sql',
                        'original_file_path': 'events.sql',
                        'root_path': get_os_path('/usr/src/app'),
                        'raw_sql': 'does not matter',
                        'agate_table': None,
                        'columns': {},
                        'description': '',
                    },
                    'model.root.dep': {
                        'name': 'dep',
                        'alias': 'dep',
                        'database': 'test',
                        'schema': 'analytics',
                        'resource_type': 'model',
                        'unique_id': 'model.root.dep',
                        'fqn': ['root', 'dep'],
                        'empty': False,
                        'package_name': 'root',
                        'refs': [['events']],
                        'sources': [],
                        'depends_on': {
                            'nodes': ['model.root.events'],
                            'macros': []
                        },
                        'config': self.model_config,
                        'tags': [],
                        'path': 'multi.sql',
                        'original_file_path': 'multi.sql',
                        'root_path': get_os_path('/usr/src/app'),
                        'raw_sql': 'does not matter',
                        'agate_table': None,
                        'columns': {},
                        'description': '',
                    }
                }
            }
        )

    def test__in_model_config(self):
        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'model_one.sql',
            'original_file_path': 'model_one.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("{{config({'materialized':'table'})}}"
                        "select * from events"),
        }]

        self.model_config.update({
            'materialized': 'table'
        })

        parser = ModelParser(
            self.root_project_config,
            self.all_projects,
            self.macro_manifest
        )

        self.assertEqual(
            parser.parse_sql_nodes(models),
            ({
                'model.root.model_one': ParsedNode(
                    alias='model_one',
                    name='model_one',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.model_one',
                    fqn=['root', 'model_one'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': [],
                    },
                    config=self.model_config,
                    tags=[],
                    root_path=get_os_path('/usr/src/app'),
                    path='model_one.sql',
                    original_file_path='model_one.sql',
                    raw_sql=self.find_input_by_name(
                        models, 'model_one').get('raw_sql'),
                    description='',
                    columns={}
                )
            }, [])
        )

    def test__root_project_config(self):
        self.root_project_config.models = {
            'materialized': 'ephemeral',
            'root': {
                'view': {
                    'materialized': 'view'
                }
            }
        }

        models = [{
            'name': 'table',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'table.sql',
            'original_file_path': 'table.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("{{config({'materialized':'table'})}}"
                        "select * from events"),
        }, {
            'name': 'ephemeral',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'ephemeral.sql',
            'original_file_path': 'ephemeral.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }, {
            'name': 'view',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'view.sql',
            'original_file_path': 'view.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }]

        self.model_config.update({
            'materialized': 'table'
        })

        ephemeral_config = self.model_config.copy()
        ephemeral_config.update({
            'materialized': 'ephemeral'
        })

        view_config = self.model_config.copy()
        view_config.update({
            'materialized': 'view'
        })

        parser = ModelParser(
            self.root_project_config,
            self.all_projects,
            self.macro_manifest
        )

        self.assertEqual(
            parser.parse_sql_nodes(models),
            ({
                'model.root.table': ParsedNode(
                    alias='table',
                    name='table',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.table',
                    fqn=['root', 'table'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    path='table.sql',
                    original_file_path='table.sql',
                    config=self.model_config,
                    tags=[],
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'table').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.root.ephemeral': ParsedNode(
                    alias='ephemeral',
                    name='ephemeral',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.ephemeral',
                    fqn=['root', 'ephemeral'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    path='ephemeral.sql',
                    original_file_path='ephemeral.sql',
                    config=ephemeral_config,
                    tags=[],
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=self.find_input_by_name(
                        models, 'ephemeral').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.root.view': ParsedNode(
                    alias='view',
                    name='view',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.view',
                    fqn=['root', 'view'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    path='view.sql',
                    original_file_path='view.sql',
                    root_path=get_os_path('/usr/src/app'),
                    config=view_config,
                    tags=[],
                    raw_sql=self.find_input_by_name(
                        models, 'ephemeral').get('raw_sql'),
                    description='',
                    columns={}
                ),
            }, [])
        )

    def test__other_project_config(self):
        self.root_project_config.models = {
            'materialized': 'ephemeral',
            'root': {
                'view': {
                    'materialized': 'view'
                }
            },
            'snowplow': {
                'enabled': False,
                'views': {
                    'materialized': 'view',
                    'multi_sort': {
                        'enabled': True,
                        'materialized': 'table'
                    }
                }
            }
        }

        self.snowplow_project_config.models = {
            'snowplow': {
                'enabled': False,
                'views': {
                    'materialized': 'table',
                    'sort': 'timestamp',
                    'multi_sort': {
                        'sort': ['timestamp', 'id'],
                    }
                }
            }
        }

        models = [{
            'name': 'table',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'table.sql',
            'original_file_path': 'table.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("{{config({'materialized':'table'})}}"
                        "select * from events"),
        }, {
            'name': 'ephemeral',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'ephemeral.sql',
            'original_file_path': 'ephemeral.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }, {
            'name': 'view',
            'resource_type': 'model',
            'package_name': 'root',
            'path': 'view.sql',
            'original_file_path': 'view.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }, {
            'name': 'disabled',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': 'disabled.sql',
            'original_file_path': 'disabled.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }, {
            'name': 'package',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': get_os_path('views/package.sql'),
            'original_file_path': get_os_path('views/package.sql'),
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }, {
            'name': 'multi_sort',
            'resource_type': 'model',
            'package_name': 'snowplow',
            'path': get_os_path('views/multi_sort.sql'),
            'original_file_path': get_os_path('views/multi_sort.sql'),
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': ("select * from events"),
        }]

        self.model_config.update({
            'materialized': 'table'
        })

        ephemeral_config = self.model_config.copy()
        ephemeral_config.update({
            'materialized': 'ephemeral'
        })

        view_config = self.model_config.copy()
        view_config.update({
            'materialized': 'view'
        })

        disabled_config = self.model_config.copy()
        disabled_config.update({
            'enabled': False,
            'materialized': 'ephemeral'
        })


        sort_config = self.model_config.copy()
        sort_config.update({
            'enabled': False,
            'materialized': 'view',
            'sort': 'timestamp',
        })

        multi_sort_config = self.model_config.copy()
        multi_sort_config.update({
            'materialized': 'table',
            'sort': ['timestamp', 'id']
        })

        parser = ModelParser(
            self.root_project_config,
            self.all_projects,
            self.macro_manifest
        )

        self.assertEqual(
            parser.parse_sql_nodes(models),
            ({
                'model.root.table': ParsedNode(
                    alias='table',
                    name='table',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.table',
                    fqn=['root', 'table'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    path='table.sql',
                    original_file_path='table.sql',
                    root_path=get_os_path('/usr/src/app'),
                    config=self.model_config,
                    tags=[],
                    raw_sql=self.find_input_by_name(
                        models, 'table').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.root.ephemeral': ParsedNode(
                    alias='ephemeral',
                    name='ephemeral',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.ephemeral',
                    fqn=['root', 'ephemeral'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    path='ephemeral.sql',
                    original_file_path='ephemeral.sql',
                    root_path=get_os_path('/usr/src/app'),
                    config=ephemeral_config,
                    tags=[],
                    raw_sql=self.find_input_by_name(
                        models, 'ephemeral').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.root.view': ParsedNode(
                    alias='view',
                    name='view',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.view',
                    fqn=['root', 'view'],
                    empty=False,
                    package_name='root',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    path='view.sql',
                    original_file_path='view.sql',
                    root_path=get_os_path('/usr/src/app'),
                    config=view_config,
                    tags=[],
                    raw_sql=self.find_input_by_name(
                        models, 'view').get('raw_sql'),
                    description='',
                    columns={}
                ),
                'model.snowplow.multi_sort': ParsedNode(
                    alias='multi_sort',
                    name='multi_sort',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.snowplow.multi_sort',
                    fqn=['snowplow', 'views', 'multi_sort'],
                    empty=False,
                    package_name='snowplow',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    path=get_os_path('views/multi_sort.sql'),
                    original_file_path=get_os_path('views/multi_sort.sql'),
                    root_path=get_os_path('/usr/src/app'),
                    config=multi_sort_config,
                    tags=[],
                    raw_sql=self.find_input_by_name(
                        models, 'multi_sort').get('raw_sql'),
                    description='',
                    columns={}
                ),
            },
            [
                ParsedNode(
                    name='disabled',
                    resource_type='model',
                    package_name='snowplow',
                    path='disabled.sql',
                    original_file_path='disabled.sql',
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=("select * from events"),
                    database='test',
                    schema='analytics',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=disabled_config,
                    tags=[],
                    empty=False,
                    alias='disabled',
                    unique_id='model.snowplow.disabled',
                    fqn=['snowplow', 'disabled'],
                    columns={}
                ),
                ParsedNode(
                    name='package',
                    resource_type='model',
                    package_name='snowplow',
                    path=get_os_path('views/package.sql'),
                    original_file_path=get_os_path('views/package.sql'),
                    root_path=get_os_path('/usr/src/app'),
                    raw_sql=("select * from events"),
                    database='test',
                    schema='analytics',
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=sort_config,
                    tags=[],
                    empty=False,
                    alias='package',
                    unique_id='model.snowplow.package',
                    fqn=['snowplow', 'views', 'package'],
                    columns={}
                )
            ])
        )

    def test__simple_data_test(self):
        tests = [{
            'name': 'no_events',
            'resource_type': 'test',
            'package_name': 'root',
            'path': 'no_events.sql',
            'original_file_path': 'no_events.sql',
            'root_path': get_os_path('/usr/src/app'),
            'raw_sql': "select * from {{ref('base')}}"
        }]

        parser = DataTestParser(
            self.root_project_config,
            self.all_projects,
            self.macro_manifest
        )

        self.assertEqual(
            parser.parse_sql_nodes(tests),
            ({
                'test.root.no_events': ParsedNode(
                    alias='no_events',
                    name='no_events',
                    database='test',
                    schema='analytics',
                    resource_type='test',
                    unique_id='test.root.no_events',
                    fqn=['root', 'no_events'],
                    empty=False,
                    package_name='root',
                    refs=[['base']],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    path='no_events.sql',
                    original_file_path='no_events.sql',
                    root_path=get_os_path('/usr/src/app'),
                    tags=[],
                    raw_sql=self.find_input_by_name(
                        tests, 'no_events').get('raw_sql'),
                    description='',
                    columns={}
                )
            }, [])
        )

    def test__simple_macro(self):
        macro_file_contents = """
{% macro simple(a, b) %}
  {{a}} + {{b}}
{% endmacro %}
"""
        parser = MacroParser(None, None)
        result = parser.parse_macro_file(
            macro_file_path='simple_macro.sql',
            macro_file_contents=macro_file_contents,
            root_path=get_os_path('/usr/src/app'),
            package_name='root',
            resource_type=NodeType.Macro)

        self.assertTrue(callable(result['macro.root.simple'].generator))


        self.assertEqual(
            result,
            {
                'macro.root.simple': ParsedMacro(**{
                    'name': 'simple',
                    'resource_type': 'macro',
                    'unique_id': 'macro.root.simple',
                    'package_name': 'root',
                    'depends_on': {
                        'macros': []
                    },
                    'original_file_path': 'simple_macro.sql',
                    'root_path': get_os_path('/usr/src/app'),
                    'tags': [],
                    'path': 'simple_macro.sql',
                    'raw_sql': macro_file_contents,
                })
            }
        )

    def test__simple_macro_used_in_model(self):
        macro_file_contents = """
{% macro simple(a, b) %}
  {{a}} + {{b}}
{% endmacro %}
"""
        parser = MacroParser(None, None)
        result = parser.parse_macro_file(
            macro_file_path='simple_macro.sql',
            macro_file_contents=macro_file_contents,
            root_path=get_os_path('/usr/src/app'),
            package_name='root',
            resource_type=NodeType.Macro)

        self.assertTrue(callable(result['macro.root.simple'].generator))

        self.assertEqual(
            result,
            {
                'macro.root.simple': ParsedMacro(**{
                    'name': 'simple',
                    'resource_type': 'macro',
                    'unique_id': 'macro.root.simple',
                    'package_name': 'root',
                    'depends_on': {
                        'macros': []
                    },
                    'original_file_path': 'simple_macro.sql',
                    'root_path': get_os_path('/usr/src/app'),
                    'tags': [],
                    'path': 'simple_macro.sql',
                    'raw_sql': macro_file_contents,
                }),
            }
        )

        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'original_file_path': 'model_one.sql',
            'root_path': get_os_path('/usr/src/app'),
            'path': 'model_one.sql',
            'raw_sql': ("select *, {{package.simple(1, 2)}} from events"),
        }]

        parser = ModelParser(
            self.root_project_config,
            self.all_projects,
            self.macro_manifest
        )

        self.assertEqual(
            parser.parse_sql_nodes(models),
            ({
                'model.root.model_one': ParsedNode(
                    alias='model_one',
                    name='model_one',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.model_one',
                    fqn=['root', 'model_one'],
                    empty=False,
                    package_name='root',
                    original_file_path='model_one.sql',
                    root_path=get_os_path('/usr/src/app'),
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='model_one.sql',
                    raw_sql=self.find_input_by_name(
                        models, 'model_one').get('raw_sql'),
                    description='',
                    columns={}
                )
            }, [])
        )

    def test__macro_no_explicit_project_used_in_model(self):
        models = [{
            'name': 'model_one',
            'resource_type': 'model',
            'package_name': 'root',
            'root_path': get_os_path('/usr/src/app'),
            'path': 'model_one.sql',
            'original_file_path': 'model_one.sql',
            'raw_sql': ("select *, {{ simple(1, 2) }} from events"),
        }]

        parser = ModelParser(
            self.root_project_config,
            self.all_projects,
            self.macro_manifest
        )

        self.assertEqual(
            parser.parse_sql_nodes(models),
            ({
                'model.root.model_one': ParsedNode(
                    alias='model_one',
                    name='model_one',
                    database='test',
                    schema='analytics',
                    resource_type='model',
                    unique_id='model.root.model_one',
                    fqn=['root', 'model_one'],
                    empty=False,
                    package_name='root',
                    root_path=get_os_path('/usr/src/app'),
                    refs=[],
                    sources=[],
                    depends_on={
                        'nodes': [],
                        'macros': []
                    },
                    config=self.model_config,
                    tags=[],
                    path='model_one.sql',
                    original_file_path='model_one.sql',
                    raw_sql=self.find_input_by_name(
                        models, 'model_one').get('raw_sql'),
                    description='',
                    columns={}
                )
            }, [])
        )
