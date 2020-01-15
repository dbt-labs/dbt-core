import pickle

from dbt.node_types import NodeType
from dbt.contracts.graph.parsed import (
    ParsedModelNode, DependsOn, NodeConfig, ColumnInfo, Hook, ParsedTestNode,
    TestConfig, ParsedSnapshotNode, TimestampSnapshotConfig, All, Docref,
    GenericSnapshotConfig, CheckSnapshotConfig, SnapshotStrategy,
    IntermediateSnapshotNode, ParsedNodePatch, ParsedMacro,
    MacroDependsOn, ParsedSourceDefinition, ParsedDocumentation, ParsedHookNode
)
from dbt.contracts.graph.unparsed import Quoting, FreshnessThreshold

from hologram import ValidationError
from .utils import ContractTestCase


class TestNodeConfig(ContractTestCase):
    ContractType = NodeConfig

    def test_basics(self):
        cfg_dict = {
            'column_types': {},
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'quoting': {},
            'tags': [],
            'vars': {},
        }
        cfg = self.ContractType()
        self.assert_symmetric(cfg, cfg_dict)

    def test_populated(self):
        cfg_dict = {
            'column_types': {'a': 'text'},
            'enabled': True,
            'materialized': 'table',
            'persist_docs': {},
            'post-hook': [{'sql': 'insert into blah(a, b) select "1", 1', 'transaction': True}],
            'pre-hook': [],
            'quoting': {},
            'tags': [],
            'vars': {},
            'extra': 'even more',
        }
        cfg = self.ContractType(
            column_types={'a': 'text'},
            materialized='table',
            post_hook=[Hook(sql='insert into blah(a, b) select "1", 1')]
        )
        cfg._extra['extra'] = 'even more'

        self.assert_symmetric(cfg, cfg_dict)
        pickle.loads(pickle.dumps(cfg))


class TestParsedModelNode(ContractTestCase):
    ContractType = ParsedModelNode

    def test_ok(self):
        node_dict = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Model),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from wherever',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'refs': [],
            'sources': [],
            'depends_on': {'macros': [], 'nodes': []},
            'database': 'test_db',
            'description': '',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': [],
            'config': {
                'column_types': {},
                'enabled': True,
                'materialized': 'view',
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'quoting': {},
                'tags': [],
                'vars': {},
            },
            'docrefs': [],
            'columns': {},
            'meta': {},
        }
        node = self.ContractType(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='select * from wherever',
            name='foo',
            resource_type=NodeType.Model,
            unique_id='model.test.foo',
            fqn=['test', 'models', 'foo'],
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            description='',
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=[],
            config=NodeConfig(),
            meta={},
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)
        self.assertTrue(node.is_refable)
        self.assertFalse(node.is_ephemeral)
        self.assertEqual(node.local_vars(), {})

        minimum = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Model),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from wherever',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'database': 'test_db',
            'schema': 'test_schema',
            'alias': 'bar',
        }
        self.assert_from_dict(node, minimum)
        pickle.loads(pickle.dumps(node))

    def test_complex(self):
        node_dict = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Model),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from {{ ref("bar") }}',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'refs': [],
            'sources': [],
            'depends_on': {'macros': [], 'nodes': ['model.test.bar']},
            'database': 'test_db',
            'description': 'My parsed node',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': ['tag'],
            'meta': {},
            'config': {
                'column_types': {'a': 'text'},
                'enabled': True,
                'materialized': 'ephemeral',
                'persist_docs': {},
                'post-hook': [{'sql': 'insert into blah(a, b) select "1", 1', 'transaction': True}],
                'pre-hook': [],
                'quoting': {},
                'tags': [],
                'vars': {'foo': 100},
            },
            'docrefs': [],
            'columns': {'a': {'name': 'a', 'description': 'a text field', 'meta': {}}},
        }

        node = self.ContractType(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='select * from {{ ref("bar") }}',
            name='foo',
            resource_type=NodeType.Model,
            unique_id='model.test.foo',
            fqn=['test', 'models', 'foo'],
            refs=[],
            sources=[],
            depends_on=DependsOn(nodes=['model.test.bar']),
            description='My parsed node',
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=['tag'],
            meta={},
            config=NodeConfig(
                column_types={'a': 'text'},
                materialized='ephemeral',
                post_hook=[Hook(sql='insert into blah(a, b) select "1", 1')],
                vars={'foo': 100},
            ),
            columns={'a': ColumnInfo('a', 'a text field', {})},
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)
        self.assertTrue(node.is_refable)
        self.assertTrue(node.is_ephemeral)
        self.assertEqual(node.local_vars(), {'foo': 100})

    def test_invalid_bad_tags(self):
        # bad top-level field
        bad_tags = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Model),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from wherever',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'refs': [],
            'sources': [],
            'depends_on': {'macros': [], 'nodes': []},
            'database': 'test_db',
            'description': 'My parsed node',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': 100,
            'config': {
                'column_types': {},
                'enabled': True,
                'materialized': None,
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'quoting': {},
                'tags': [],
                'vars': {},
            },
            'docrefs': [],
            'columns': {},
            'meta': {},
        }
        self.assert_fails_validation(bad_tags)

    def test_invalid_bad_materialized(self):
        # bad nested field
        bad_materialized = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Model),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from wherever',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'refs': [],
            'sources': [],
            'depends_on': {'macros': [], 'nodes': []},
            'database': 'test_db',
            'description': 'My parsed node',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': ['tag'],
            'config': {
                'column_types': {},
                'enabled': True,
                'materialized': None,
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'quoting': {},
                'tags': [],
                'vars': {},
            },
            'docrefs': [],
            'columns': {},
            'meta': {},
        }
        self.assert_fails_validation(bad_materialized)

    def test_patch_ok(self):
        initial = self.ContractType(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='select * from wherever',
            name='foo',
            resource_type=NodeType.Model,
            unique_id='model.test.foo',
            fqn=['test', 'models', 'foo'],
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            description='',
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=[],
            meta={},
            config=NodeConfig(),
        )
        patch = ParsedNodePatch(
            name='foo',
            yaml_key='models',
            package_name='test',
            description='The foo model',
            original_file_path='/path/to/schema.yml',
            columns={'a': ColumnInfo(name='a', description='a text field', meta={})},
            docrefs=[
                Docref(documentation_name='foo', documentation_package='test'),
            ],
            meta={},
        )

        initial.patch(patch)

        expected_dict = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Model),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from wherever',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'refs': [],
            'sources': [],
            'depends_on': {'macros': [], 'nodes': []},
            'database': 'test_db',
            'description': 'The foo model',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': [],
            'meta': {},
            'config': {
                'column_types': {},
                'enabled': True,
                'materialized': 'view',
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'quoting': {},
                'tags': [],
                'vars': {},
            },
            'patch_path': '/path/to/schema.yml',
            'columns': {'a': {'name': 'a', 'description': 'a text field', 'meta':{}}},
            'docrefs': [
                {
                    'documentation_name': 'foo',
                    'documentation_package': 'test',
                }
            ],
        }

        expected = self.ContractType(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='select * from wherever',
            name='foo',
            resource_type=NodeType.Model,
            unique_id='model.test.foo',
            fqn=['test', 'models', 'foo'],
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            description='The foo model',
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=[],
            meta={},
            config=NodeConfig(),
            patch_path='/path/to/schema.yml',
            columns={'a': ColumnInfo(name='a', description='a text field', meta={})},
            docrefs=[
                Docref(documentation_name='foo', documentation_package='test'),
            ],
        )
        self.assert_symmetric(expected, expected_dict)  # sanity check
        self.assertEqual(initial, expected)
        self.assert_symmetric(initial, expected_dict)

    def patch_invalid(self):
        initial = self.ContractType(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='select * from wherever',
            name='foo',
            resource_type=NodeType.Model,
            unique_id='model.test.foo',
            fqn=['test', 'models', 'foo'],
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            description='',
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=[],
            config=NodeConfig(),
        )
        # invalid patch: description can't be None
        patch = ParsedNodePatch(
            name='foo',
            yaml_key='models',
            package_name='test',
            description=None,
            original_file_path='/path/to/schema.yml',
            columns={},
            docrefs=[],
        )
        with self.assertRaises(ValidationError):
            initial.patch(patch)


class TestParsedHookNode(ContractTestCase):
    ContractType = ParsedHookNode

    def test_ok(self):
        node_dict = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Operation),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from wherever',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'refs': [],
            'sources': [],
            'depends_on': {'macros': [], 'nodes': []},
            'database': 'test_db',
            'description': '',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': [],
            'config': {
                'column_types': {},
                'enabled': True,
                'materialized': 'view',
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'quoting': {},
                'tags': [],
                'vars': {},
            },
            'docrefs': [],
            'columns': {},
            'meta': {},
        }
        node = self.ContractType(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='select * from wherever',
            name='foo',
            resource_type=NodeType.Operation,
            unique_id='model.test.foo',
            fqn=['test', 'models', 'foo'],
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            description='',
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=[],
            config=NodeConfig(),
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)
        self.assertFalse(node.is_refable)
        self.assertEqual(node.get_materialization(), 'view')

        minimum = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Operation),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from wherever',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'database': 'test_db',
            'schema': 'test_schema',
            'alias': 'bar',
        }
        self.assert_from_dict(node, minimum)
        pickle.loads(pickle.dumps(node))

    def test_complex(self):
        node_dict = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Operation),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from {{ ref("bar") }}',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'refs': [],
            'sources': [],
            'depends_on': {'macros': [], 'nodes': ['model.test.bar']},
            'database': 'test_db',
            'description': 'My parsed node',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': ['tag'],
            'meta': {},
            'config': {
                'column_types': {'a': 'text'},
                'enabled': True,
                'materialized': 'table',
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'quoting': {},
                'tags': [],
                'vars': {},
            },
            'docrefs': [],
            'columns': {'a': {'name': 'a', 'description': 'a text field', 'meta':{}}},
            'index': 13,
        }

        node = self.ContractType(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='select * from {{ ref("bar") }}',
            name='foo',
            resource_type=NodeType.Operation,
            unique_id='model.test.foo',
            fqn=['test', 'models', 'foo'],
            refs=[],
            sources=[],
            depends_on=DependsOn(nodes=['model.test.bar']),
            description='My parsed node',
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=['tag'],
            meta={},
            config=NodeConfig(
                column_types={'a': 'text'},
                materialized='table',
                post_hook=[]
            ),
            columns={'a': ColumnInfo('a', 'a text field', {})},
            index=13,
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)
        self.assertFalse(node.is_refable)
        self.assertEqual(node.get_materialization(), 'table')

    def test_invalid_index_type(self):
        # bad top-level field
        bad_index = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Operation),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from wherever',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'refs': [],
            'sources': [],
            'depends_on': {'macros': [], 'nodes': []},
            'database': 'test_db',
            'description': 'My parsed node',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': [],
            'config': {
                'column_types': {},
                'enabled': True,
                'materialized': None,
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'quoting': {},
                'tags': [],
                'vars': {},
            },
            'docrefs': [],
            'columns': {},
            'meta': {},
            'index': 'a string!?',
        }
        self.assert_fails_validation(bad_index)


class TestParsedTestNode(ContractTestCase):
    ContractType = ParsedTestNode

    def test_ok(self):
        node_dict = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Test),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from wherever',
            'unique_id': 'test.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'refs': [],
            'sources': [],
            'depends_on': {'macros': [], 'nodes': []},
            'database': 'test_db',
            'description': '',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': [],
            'meta': {},
            'config': {
                'column_types': {},
                'enabled': True,
                'materialized': 'view',
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'quoting': {},
                'tags': [],
                'vars': {},
                'severity': 'error',
            },
            'docrefs': [],
            'columns': {},
            'meta': {},
        }
        node = self.ContractType(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='select * from wherever',
            name='foo',
            resource_type=NodeType.Test,
            unique_id='test.test.foo',
            fqn=['test', 'models', 'foo'],
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            description='',
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=[],
            meta={},
            config=TestConfig(),
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)
        self.assertFalse(node.is_ephemeral)
        self.assertFalse(node.is_refable)
        self.assertEqual(node.get_materialization(), 'view')

        minimum = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Test),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from wherever',
            'unique_id': 'test.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'database': 'test_db',
            'schema': 'test_schema',
            'alias': 'bar',
            'meta': {},
        }
        self.assert_from_dict(node, minimum)
        pickle.loads(pickle.dumps(node))

    def test_complex(self):
        node_dict = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Test),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from {{ ref("bar") }}',
            'unique_id': 'test.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'refs': [],
            'sources': [],
            'depends_on': {'macros': [], 'nodes': ['model.test.bar']},
            'database': 'test_db',
            'description': 'My parsed node',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': ['tag'],
            'meta': {},
            'config': {
                'column_types': {'a': 'text'},
                'enabled': True,
                'materialized': 'table',
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'quoting': {},
                'tags': [],
                'vars': {},
                'severity': 'WARN',
                'extra_key': 'extra value'
            },
            'docrefs': [],
            'columns': {'a': {'name': 'a', 'description': 'a text field', 'meta': {}}},
            'column_name': 'id',
        }

        cfg = TestConfig(
            column_types={'a': 'text'},
            materialized='table',
            severity='WARN'
        )
        cfg._extra.update({'extra_key': 'extra value'})

        node = self.ContractType(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='select * from {{ ref("bar") }}',
            name='foo',
            resource_type=NodeType.Test,
            unique_id='test.test.foo',
            fqn=['test', 'models', 'foo'],
            refs=[],
            sources=[],
            depends_on=DependsOn(nodes=['model.test.bar']),
            description='My parsed node',
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=['tag'],
            meta={},
            config=cfg,
            columns={'a': ColumnInfo('a', 'a text field',{})},
            column_name='id',
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)

    def test_invalid_column_name_type(self):
        # bad top-level field
        bad_column_name = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Test),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from wherever',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'refs': [],
            'sources': [],
            'depends_on': {'macros': [], 'nodes': []},
            'database': 'test_db',
            'description': 'My parsed node',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': 100,
            'config': {
                'column_types': {},
                'enabled': True,
                'materialized': None,
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'quoting': {},
                'tags': [],
                'vars': {},
                'severity': 'ERROR',
            },
            'docrefs': [],
            'columns': {},
            'column_name': {},
            'meta': {},
        }
        self.assert_fails_validation(bad_column_name)

    def test_invalid_missing_severity(self):
        # note the typo ('severtiy')
        missing_config_value = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Test),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from wherever',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'refs': [],
            'sources': [],
            'depends_on': {'macros': [], 'nodes': []},
            'database': 'test_db',
            'description': 'My parsed node',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': ['tag'],
            'config': {
                'column_types': {},
                'enabled': True,
                'materialized': None,
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'quoting': {},
                'tags': [],
                'vars': {},
                'severtiy': 'WARN',
            },
            'docrefs': [],
            'columns': {},
            'meta': {},
        }
        self.assert_fails_validation(missing_config_value)

    def test_invalid_severity(self):
        invalid_config_value = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Test),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from wherever',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'refs': [],
            'sources': [],
            'depends_on': {'macros': [], 'nodes': []},
            'database': 'test_db',
            'description': 'My parsed node',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': ['tag'],
            'config': {
                'column_types': {},
                'enabled': True,
                'materialized': None,
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'quoting': {},
                'tags': [],
                'vars': {},
                'severity': 'WERROR',  # invalid severity
            },
            'docrefs': [],
            'columns': {},
            'meta': {},
        }
        self.assert_fails_validation(invalid_config_value)


class TestTimestampSnapshotConfig(ContractTestCase):
    ContractType = TimestampSnapshotConfig

    def test_basics(self):
        cfg_dict = {
            'column_types': {},
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'quoting': {},
            'tags': [],
            'vars': {},
            'unique_key': 'id',
            'strategy': 'timestamp',
            'updated_at': 'last_update',
            'target_database': 'some_snapshot_db',
            'target_schema': 'some_snapshot_schema',
        }
        cfg = self.ContractType(
            strategy=SnapshotStrategy.Timestamp,
            updated_at='last_update',
            unique_key='id',
            target_database='some_snapshot_db',
            target_schema='some_snapshot_schema',
        )
        self.assert_symmetric(cfg, cfg_dict)
        pickle.loads(pickle.dumps(cfg))

    def test_populated(self):
        cfg_dict = {
            'column_types': {'a': 'text'},
            'enabled': True,
            'materialized': 'table',
            'persist_docs': {},
            'post-hook': [{'sql': 'insert into blah(a, b) select "1", 1', 'transaction': True}],
            'pre-hook': [],
            'quoting': {},
            'tags': [],
            'vars': {},
            'target_database': 'some_snapshot_db',
            'target_schema': 'some_snapshot_schema',
            'unique_key': 'id',
            'extra': 'even more',
            'strategy': 'timestamp',
            'updated_at': 'last_update',
        }
        cfg = self.ContractType(
            column_types={'a': 'text'},
            materialized='table',
            post_hook=[Hook(sql='insert into blah(a, b) select "1", 1')],
            strategy=SnapshotStrategy.Timestamp,
            target_database='some_snapshot_db',
            target_schema='some_snapshot_schema',
            updated_at='last_update',
            unique_key='id',
        )
        cfg._extra['extra'] = 'even more'

        self.assert_symmetric(cfg, cfg_dict)

    def test_invalid_wrong_strategy(self):
        bad_type = {
            'column_types': {},
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'quoting': {},
            'tags': [],
            'vars': {},
            'target_database': 'some_snapshot_db',
            'target_schema': 'some_snapshot_schema',
            'unique_key': 'id',
            'strategy': 'check',
            'updated_at': 'last_update',
        }
        self.assert_fails_validation(bad_type)

    def test_invalid_missing_updated_at(self):
        bad_fields = {
            'column_types': {},
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'quoting': {},
            'tags': [],
            'vars': {},
            'target_database': 'some_snapshot_db',
            'target_schema': 'some_snapshot_schema',
            'unique_key': 'id',
            'strategy': 'timestamp',
            'check_cols': 'all'
        }
        self.assert_fails_validation(bad_fields)


class TestCheckSnapshotConfig(ContractTestCase):
    ContractType = CheckSnapshotConfig

    def test_basics(self):
        cfg_dict = {
            'column_types': {},
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'quoting': {},
            'tags': [],
            'vars': {},
            'target_database': 'some_snapshot_db',
            'target_schema': 'some_snapshot_schema',
            'unique_key': 'id',
            'strategy': 'check',
            'check_cols': 'all',
        }
        cfg = self.ContractType(
            strategy=SnapshotStrategy.Check,
            check_cols=All.All,
            unique_key='id',
            target_database='some_snapshot_db',
            target_schema='some_snapshot_schema',
        )
        self.assert_symmetric(cfg, cfg_dict)
        pickle.loads(pickle.dumps(cfg))

    def test_populated(self):
        cfg_dict = {
            'column_types': {'a': 'text'},
            'enabled': True,
            'materialized': 'table',
            'persist_docs': {},
            'post-hook': [{'sql': 'insert into blah(a, b) select "1", 1', 'transaction': True}],
            'pre-hook': [],
            'quoting': {},
            'tags': [],
            'vars': {},
            'target_database': 'some_snapshot_db',
            'target_schema': 'some_snapshot_schema',
            'unique_key': 'id',
            'extra': 'even more',
            'strategy': 'check',
            'check_cols': ['a', 'b'],
        }
        cfg = self.ContractType(
            column_types={'a': 'text'},
            materialized='table',
            post_hook=[Hook(sql='insert into blah(a, b) select "1", 1')],
            strategy=SnapshotStrategy.Check,
            check_cols=['a', 'b'],
            target_database='some_snapshot_db',
            target_schema='some_snapshot_schema',
            unique_key='id',
        )
        cfg._extra['extra'] = 'even more'

        self.assert_symmetric(cfg, cfg_dict)

    def test_invalid_wrong_strategy(self):
        wrong_strategy = {
            'column_types': {},
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'quoting': {},
            'tags': [],
            'vars': {},
            'target_database': 'some_snapshot_db',
            'target_schema': 'some_snapshot_schema',
            'unique_key': 'id',
            'strategy': 'timestamp',
            'check_cols': 'all',
        }
        self.assert_fails_validation(wrong_strategy)

    def test_invalid_missing_check_cols(self):
        wrong_fields = {
            'column_types': {},
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'quoting': {},
            'tags': [],
            'vars': {},
            'target_database': 'some_snapshot_db',
            'target_schema': 'some_snapshot_schema',
            'unique_key': 'id',
            'strategy': 'check',
            'updated_at': 'last_update'
        }
        self.assert_fails_validation(wrong_fields)

    def test_invalid_check_value(self):
        invalid_check_type = {
            'column_types': {},
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'quoting': {},
            'tags': [],
            'vars': {},
            'target_database': 'some_snapshot_db',
            'target_schema': 'some_snapshot_schema',
            'unique_key': 'id',
            'strategy': 'timestamp',
            'check_cols': 'some',
        }
        self.assert_fails_validation(invalid_check_type)


class TestParsedSnapshotNode(ContractTestCase):
    ContractType = ParsedSnapshotNode

    def test_timestamp_ok(self):
        node_dict = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Snapshot),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from wherever',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'refs': [],
            'sources': [],
            'depends_on': {'macros': [], 'nodes': []},
            'database': 'test_db',
            'description': '',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': [],
            'config': {
                'column_types': {},
                'enabled': True,
                'materialized': 'view',
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'quoting': {},
                'tags': [],
                'vars': {},
                'target_database': 'some_snapshot_db',
                'target_schema': 'some_snapshot_schema',
                'unique_key': 'id',
                'strategy': 'timestamp',
                'updated_at': 'last_update',
            },
            'docrefs': [],
            'columns': {},
            'meta': {},
        }

        node = self.ContractType(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='select * from wherever',
            name='foo',
            resource_type=NodeType.Snapshot,
            unique_id='model.test.foo',
            fqn=['test', 'models', 'foo'],
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            description='',
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=[],
            config=TimestampSnapshotConfig(
                strategy=SnapshotStrategy.Timestamp,
                unique_key='id',
                updated_at='last_update',
                target_database='some_snapshot_db',
                target_schema='some_snapshot_schema',
            ),
        )

        cfg = NodeConfig()
        cfg._extra.update({
            'unique_key': 'id',
            'strategy': 'timestamp',
            'updated_at': 'last_update',
            'target_database': 'some_snapshot_db',
            'target_schema': 'some_snapshot_schema',
        })

        inter = IntermediateSnapshotNode(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='select * from wherever',
            name='foo',
            resource_type=NodeType.Snapshot,
            unique_id='model.test.foo',
            fqn=['test', 'models', 'foo'],
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            description='',
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=[],
            config=cfg,
        )
        self.assert_symmetric(node, node_dict)
        self.assert_symmetric(inter, node_dict, cls=IntermediateSnapshotNode)
        self.assertEqual(
            self.ContractType.from_dict(inter.to_dict()),
            node
        )
        self.assertTrue(node.is_refable)
        self.assertFalse(node.is_ephemeral)
        pickle.loads(pickle.dumps(node))

    def test_check_ok(self):
        node_dict = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Snapshot),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from wherever',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'refs': [],
            'sources': [],
            'depends_on': {'macros': [], 'nodes': []},
            'database': 'test_db',
            'description': '',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': [],
            'config': {
                'column_types': {},
                'enabled': True,
                'materialized': 'view',
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'quoting': {},
                'tags': [],
                'vars': {},
                'target_database': 'some_snapshot_db',
                'target_schema': 'some_snapshot_schema',
                'unique_key': 'id',
                'strategy': 'check',
                'check_cols': 'all',
            },
            'docrefs': [],
            'columns': {},
            'meta': {},
        }

        node = self.ContractType(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='select * from wherever',
            name='foo',
            resource_type=NodeType.Snapshot,
            unique_id='model.test.foo',
            fqn=['test', 'models', 'foo'],
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            description='',
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=[],
            config=CheckSnapshotConfig(
                strategy=SnapshotStrategy.Check,
                unique_key='id',
                check_cols=All.All,
                target_database='some_snapshot_db',
                target_schema='some_snapshot_schema',
            ),
        )
        cfg = NodeConfig()
        cfg._extra.update({
            'unique_key': 'id',
            'strategy': 'check',
            'check_cols': 'all',
            'target_database': 'some_snapshot_db',
            'target_schema': 'some_snapshot_schema',
        })

        inter = IntermediateSnapshotNode(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='select * from wherever',
            name='foo',
            resource_type=NodeType.Snapshot,
            unique_id='model.test.foo',
            fqn=['test', 'models', 'foo'],
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            description='',
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=[],
            config=cfg,
        )
        self.assert_symmetric(node, node_dict)
        self.assert_symmetric(inter, node_dict, cls=IntermediateSnapshotNode)
        self.assertEqual(
            self.ContractType.from_dict(inter.to_dict()),
            node
        )
        self.assertTrue(node.is_refable)
        self.assertFalse(node.is_ephemeral)

    def test_invalid_bad_resource_type(self):
        bad_resource_type = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Model),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from wherever',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'refs': [],
            'sources': [],
            'depends_on': {'macros': [], 'nodes': []},
            'database': 'test_db',
            'description': '',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': [],
            'config': {
                'column_types': {},
                'enabled': True,
                'materialized': 'view',
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'quoting': {},
                'tags': [],
                'vars': {},
                'target_database': 'some_snapshot_db',
                'target_schema': 'some_snapshot_schema',
                'unique_key': 'id',
                'strategy': 'timestamp',
                'updated_at': 'last_update',
            },
            'docrefs': [],
            'columns': {},
            'meta': {},
        }
        self.assert_fails_validation(bad_resource_type)


class TestParsedNodePatch(ContractTestCase):
    ContractType = ParsedNodePatch

    def test_empty(self):
        dct = {
            'name': 'foo',
            'description': 'The foo model',
            'original_file_path': '/path/to/schema.yml',
            'columns': {},
            'docrefs': [],
            'meta': {},
            'yaml_key': 'models',
            'package_name': 'test',
        }
        patch = self.ContractType(
            name='foo',
            description='The foo model',
            yaml_key='models',
            package_name='test',
            original_file_path='/path/to/schema.yml',
            columns={},
            docrefs=[],
            meta={},
        )
        self.assert_symmetric(patch, dct)

    def test_populated(self):
        dct = {
            'name': 'foo',
            'description': 'The foo model',
            'original_file_path': '/path/to/schema.yml',
            'columns': {'a': {'name': 'a', 'description': 'a text field', 'meta':{}}},
            'docrefs': [
                {
                    'documentation_name': 'foo',
                    'documentation_package': 'test',
                }
            ],
            'meta': {'key': ['value']},
            'yaml_key': 'models',
            'package_name': 'test',
        }
        patch = self.ContractType(
            name='foo',
            description='The foo model',
            original_file_path='/path/to/schema.yml',
            columns={'a': ColumnInfo(name='a', description='a text field', meta={})},
            docrefs=[
                Docref(documentation_name='foo', documentation_package='test'),
            ],
            meta={'key': ['value']},
            yaml_key='models',
            package_name='test',
        )
        self.assert_symmetric(patch, dct)
        pickle.loads(pickle.dumps(patch))


class TestParsedMacro(ContractTestCase):
    ContractType = ParsedMacro

    def test_ok(self):
        macro_dict = {
            'name': 'foo',
            'path': '/root/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': '{% macro foo() %}select 1 as id{% endmacro %}',
            'root_path': '/root/',
            'resource_type': 'macro',
            'unique_id': 'macro.test.foo',
            'tags': [],
            'depends_on': {'macros': []}
        }
        macro = ParsedMacro(
            name='foo',
            path='/root/path.sql',
            original_file_path='/root/path.sql',
            package_name='test',
            raw_sql='{% macro foo() %}select 1 as id{% endmacro %}',
            root_path='/root/',
            resource_type=NodeType.Macro,
            unique_id='macro.test.foo',
            tags=[],
            depends_on=MacroDependsOn()
        )
        self.assert_symmetric(macro, macro_dict)
        self.assertEqual(macro.local_vars(), {})
        pickle.loads(pickle.dumps(macro))

    def test_invalid_missing_unique_id(self):
        bad_missing_uid = {
            'name': 'foo',
            'path': '/root/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': '{% macro foo() %}select 1 as id{% endmacro %}',
            'root_path': '/root/',
            'resource_type': 'macro',
            'tags': [],
            'depends_on': {'macros': []}
        }
        self.assert_fails_validation(bad_missing_uid)

    def test_invalid_extra_field(self):
        bad_extra_field = {
            'name': 'foo',
            'path': '/root/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': '{% macro foo() %}select 1 as id{% endmacro %}',
            'root_path': '/root/',
            'resource_type': 'macro',
            'unique_id': 'macro.test.foo',
            'tags': [],
            'depends_on': {'macros': []},
            'extra': 'too many fields'
        }
        self.assert_fails_validation(bad_extra_field)


class TestParsedDocumentation(ContractTestCase):
    ContractType = ParsedDocumentation

    def test_ok(self):
        doc_dict = {
            'block_contents': 'some doc contents',
            'file_contents': '{% doc foo %}some doc contents{% enddoc %}',
            'name': 'foo',
            'original_file_path': '/root/docs/doc.md',
            'package_name': 'test',
            'path': '/root/docs',
            'root_path': '/root',
            'unique_id': 'test.foo',
        }
        doc = self.ContractType(
            package_name='test',
            root_path='/root',
            path='/root/docs',
            original_file_path='/root/docs/doc.md',
            file_contents='{% doc foo %}some doc contents{% enddoc %}',
            name='foo',
            unique_id='test.foo',
            block_contents='some doc contents'
        )
        self.assert_symmetric(doc, doc_dict)
        pickle.loads(pickle.dumps(doc))

    def test_invalid_missing(self):
        bad_missing_contents = {
            # 'block_contents': 'some doc contents',
            'file_contents': '{% doc foo %}some doc contents{% enddoc %}',
            'name': 'foo',
            'original_file_path': '/root/docs/doc.md',
            'package_name': 'test',
            'path': '/root/docs',
            'root_path': '/root',
            'unique_id': 'test.foo',
        }
        self.assert_fails_validation(bad_missing_contents)

    def test_invalid_extra(self):
        bad_extra_field = {
            'block_contents': 'some doc contents',
            'file_contents': '{% doc foo %}some doc contents{% enddoc %}',
            'name': 'foo',
            'original_file_path': '/root/docs/doc.md',
            'package_name': 'test',
            'path': '/root/docs',
            'root_path': '/root',
            'unique_id': 'test.foo',

            'extra': 'more',
        }
        self.assert_fails_validation(bad_extra_field)


class TestParsedSourceDefinition(ContractTestCase):
    ContractType = ParsedSourceDefinition

    def test_basic(self):
        source_def_dict = {
            'package_name': 'test',
            'root_path': '/root',
            'path': '/root/models/sources.yml',
            'original_file_path': '/root/models/sources.yml',
            'database': 'some_db',
            'schema': 'some_schema',
            'fqn': ['test', 'source', 'my_source', 'my_source_table'],
            'source_name': 'my_source',
            'name': 'my_source_table',
            'source_description': 'my source description',
            'loader': 'stitch',
            'identifier': 'my_source_table',
            'resource_type': str(NodeType.Source),
            'description': '',
            'docrefs': [],
            'columns': {},
            'quoting': {},
            'unique_id': 'test.source.my_source.my_source_table',
            'meta': {},
            'source_meta': {},
        }
        source_def = self.ContractType(
            columns={},
            docrefs=[],
            database='some_db',
            description='',
            fqn=['test', 'source', 'my_source', 'my_source_table'],
            identifier='my_source_table',
            loader='stitch',
            name='my_source_table',
            original_file_path='/root/models/sources.yml',
            package_name='test',
            path='/root/models/sources.yml',
            quoting=Quoting(),
            resource_type=NodeType.Source,
            root_path='/root',
            schema='some_schema',
            source_description='my source description',
            source_name='my_source',
            unique_id='test.source.my_source.my_source_table',
        )
        self.assert_symmetric(source_def, source_def_dict)
        minimum = {
            'package_name': 'test',
            'root_path': '/root',
            'path': '/root/models/sources.yml',
            'original_file_path': '/root/models/sources.yml',
            'database': 'some_db',
            'schema': 'some_schema',
            'fqn': ['test', 'source', 'my_source', 'my_source_table'],
            'source_name': 'my_source',
            'name': 'my_source_table',
            'source_description': 'my source description',
            'loader': 'stitch',
            'identifier': 'my_source_table',
            'resource_type': str(NodeType.Source),
            'unique_id': 'test.source.my_source.my_source_table',
        }
        self.assert_from_dict(source_def, minimum)
        pickle.loads(pickle.dumps(source_def))

    def test_invalid_missing(self):
        bad_missing_name = {
            'package_name': 'test',
            'root_path': '/root',
            'path': '/root/models/sources.yml',
            'original_file_path': '/root/models/sources.yml',
            'database': 'some_db',
            'schema': 'some_schema',
            'fqn': ['test', 'source', 'my_source', 'my_source_table'],
            'source_name': 'my_source',
            # 'name': 'my_source_table',
            'source_description': 'my source description',
            'loader': 'stitch',
            'identifier': 'my_source_table',
            'resource_type': str(NodeType.Source),
            'unique_id': 'test.source.my_source.my_source_table',
        }
        self.assert_fails_validation(bad_missing_name)

    def test_invalid_bad_resource_type(self):
        bad_resource_type = {
            'package_name': 'test',
            'root_path': '/root',
            'path': '/root/models/sources.yml',
            'original_file_path': '/root/models/sources.yml',
            'database': 'some_db',
            'schema': 'some_schema',
            'fqn': ['test', 'source', 'my_source', 'my_source_table'],
            'source_name': 'my_source',
            'name': 'my_source_table',
            'source_description': 'my source description',
            'loader': 'stitch',
            'identifier': 'my_source_table',
            'resource_type': str(NodeType.Model),
            'unique_id': 'test.source.my_source.my_source_table',
        }
        self.assert_fails_validation(bad_resource_type)
