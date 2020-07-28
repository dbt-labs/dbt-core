import pickle

from dbt.node_types import NodeType
from dbt.contracts.graph.model_config import (
    All,
    NodeConfig,
    TestConfig,
    TimestampSnapshotConfig,
    CheckSnapshotConfig,
    SourceConfig,
    EmptySnapshotConfig,
    SnapshotStrategy,
    Hook,
)
from dbt.contracts.graph.parsed import (
    ParsedModelNode,
    DependsOn,
    ColumnInfo,
    ParsedSchemaTestNode,
    ParsedSnapshotNode,
    IntermediateSnapshotNode,
    ParsedNodePatch,
    ParsedMacro,
    Docs,
    MacroDependsOn,
    ParsedSourceDefinition,
    ParsedDocumentation,
    ParsedHookNode,
    TestMetadata,
)
from dbt.contracts.graph.unparsed import Quoting

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

    def _model_ok(self):
        return {
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
            'deferred': False,
            'docs': {'show': True},
            'columns': {},
            'meta': {},
        }


    def test_ok(self):
        node_dict = self._model_ok()
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
            'deferred': True,
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
            'docs': {'show': True},
            'columns': {
                'a': {
                    'name': 'a',
                    'description': 'a text field',
                    'meta': {},
                    'tags': [],
                },
            },
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
            deferred=True,
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
        bad_tags = self._model_ok()
        bad_tags['tags'] = 100
        self.assert_fails_validation(bad_tags)

    def test_invalid_bad_materialized(self):
        # bad nested field
        bad_materialized = self._model_ok()
        bad_materialized['config']['materialized'] = None
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
            docs=Docs(),
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
            'deferred': False,
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
            'columns': {
                'a': {
                    'name': 'a',
                    'description': 'a text field',
                    'meta': {},
                    'tags': [],
                },
            },
            'docs': {'show': True},
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
            docs=Docs(),
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
            docs=Docs(),
        )
        with self.assertRaises(ValidationError):
            initial.patch(patch)


class TestParsedHookNode(ContractTestCase):
    ContractType = ParsedHookNode

    def _hook_ok(self):
        return {
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
            'deferred': False,
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
            'docs': {'show': True},
            'columns': {},
            'meta': {},
            'index': 10,
        }

    def test_ok(self):
        node_dict = self._hook_ok()
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
            deferred=False,
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=[],
            config=NodeConfig(),
            index=10,
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)
        self.assertFalse(node.is_refable)
        self.assertEqual(node.get_materialization(), 'view')

        node.index = None
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
            'deferred': False,
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
            'docs': {'show': True},
            'columns': {
                'a': {
                    'name': 'a',
                    'description': 'a text field',
                    'meta': {},
                    'tags': [],
                },
            },
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
            deferred=False,
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
        bad_index = self._hook_ok()
        bad_index['index'] = 'a string!?'
        self.assert_fails_validation(bad_index)


class TestParsedSchemaTestNode(ContractTestCase):
    ContractType = ParsedSchemaTestNode

    def _minimum(self):
        return {
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
            'test_metadata': {
                'name': 'foo',
                'kwargs': {},
            },
        }

    def _complex(self):
        return {
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
            'deferred': False,
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
            'docs': {'show': False},
            'columns': {
                'a': {
                    'name': 'a',
                    'description': 'a text field',
                    'meta': {},
                    'tags': [],
                },
            },
            'column_name': 'id',
            'test_metadata': {
                'name': 'foo',
                'kwargs': {},
            },
        }

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
            'deferred': False,
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
                'severity': 'ERROR',
            },
            'docs': {'show': True},
            'columns': {},
            'test_metadata': {
                'name': 'foo',
                'kwargs': {},
            },
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
            test_metadata=TestMetadata(namespace=None, name='foo', kwargs={}),
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)
        self.assertFalse(node.is_ephemeral)
        self.assertFalse(node.is_refable)
        self.assertEqual(node.get_materialization(), 'view')

        minimum = self._minimum()
        self.assert_from_dict(node, minimum)
        pickle.loads(pickle.dumps(node))

    def test_complex(self):
        node_dict = self._complex()

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
            docs=Docs(show=False),
            test_metadata=TestMetadata(namespace=None, name='foo', kwargs={}),
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)

    def test_invalid_column_name_type(self):
        # bad top-level field
        bad_column_name = self._complex()
        bad_column_name['column_name'] = {}
        self.assert_fails_validation(bad_column_name)

    def test_invalid_severity(self):
        invalid_config_value = self._complex()
        invalid_config_value['config']['severity'] = 'WERROR'
        self.assert_fails_validation(invalid_config_value)


class TestTimestampSnapshotConfig(ContractTestCase):
    ContractType = TimestampSnapshotConfig

    def _cfg_basic(self):
        return {
            'column_types': {},
            'enabled': True,
            'materialized': 'snapshot',
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

    def test_basics(self):
        cfg_dict = self._cfg_basic()
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
            'materialized': 'snapshot',
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
            materialized='snapshot',
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
        bad_type = self._cfg_basic()
        bad_type['strategy'] = 'check'
        self.assert_fails_validation(bad_type)

    def test_invalid_missing_updated_at(self):
        bad_fields = self._cfg_basic()
        del bad_fields['updated_at']
        bad_fields['check_cols'] = 'all'
        self.assert_fails_validation(bad_fields)


class TestCheckSnapshotConfig(ContractTestCase):
    ContractType = CheckSnapshotConfig

    def _cfg_ok(self):
        return {
            'column_types': {},
            'enabled': True,
            'materialized': 'snapshot',
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

    def test_basics(self):
        cfg_dict = self._cfg_ok()
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
            'materialized': 'snapshot',
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
            materialized='snapshot',
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
        wrong_strategy = self._cfg_ok()
        wrong_strategy['strategy'] = 'timestamp'
        self.assert_fails_validation(wrong_strategy)

    def test_invalid_missing_check_cols(self):
        wrong_fields = self._cfg_ok()
        del wrong_fields['check_cols']
        self.assert_fails_validation(wrong_fields)

    def test_invalid_check_value(self):
        invalid_check_type = self._cfg_ok()
        invalid_check_type['check_cols'] = 'some'
        self.assert_fails_validation(invalid_check_type)


class TestParsedSnapshotNode(ContractTestCase):
    ContractType = ParsedSnapshotNode

    def _ts_ok(self):
        return {
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
            'deferred': False,
            'database': 'test_db',
            'description': '',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': [],
            'config': {
                'column_types': {},
                'enabled': True,
                'materialized': 'snapshot',
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
            'docs': {'show': True},
            'columns': {},
            'meta': {},
        }

    def test_timestamp_ok(self):
        node_dict = self._ts_ok()

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

        cfg = EmptySnapshotConfig()
        cfg._extra.update({
            'strategy': 'timestamp',
            'unique_key': 'id',
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
            'deferred': False,
            'description': '',
            'schema': 'test_schema',
            'alias': 'bar',
            'tags': [],
            'config': {
                'column_types': {},
                'enabled': True,
                'materialized': 'snapshot',
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
            'docs': {'show': True},
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
        cfg = EmptySnapshotConfig()
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
        bad_resource_type = self._ts_ok()
        bad_resource_type['resource_type'] = str(NodeType.Model)
        self.assert_fails_validation(bad_resource_type)


class TestParsedNodePatch(ContractTestCase):
    ContractType = ParsedNodePatch

    def test_empty(self):
        dct = {
            'name': 'foo',
            'description': 'The foo model',
            'original_file_path': '/path/to/schema.yml',
            'columns': {},
            'docs': {'show': True},
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
            docs=Docs(),
            meta={},
        )
        self.assert_symmetric(patch, dct)

    def test_populated(self):
        dct = {
            'name': 'foo',
            'description': 'The foo model',
            'original_file_path': '/path/to/schema.yml',
            'columns': {
                'a': {
                    'name': 'a',
                    'description': 'a text field',
                    'meta': {},
                    'tags': [],
                },
            },
            'docs': {'show': False},
            'meta': {'key': ['value']},
            'yaml_key': 'models',
            'package_name': 'test',
        }
        patch = self.ContractType(
            name='foo',
            description='The foo model',
            original_file_path='/path/to/schema.yml',
            columns={'a': ColumnInfo(name='a', description='a text field', meta={})},
            meta={'key': ['value']},
            yaml_key='models',
            package_name='test',
            docs=Docs(show=False),
        )
        self.assert_symmetric(patch, dct)
        pickle.loads(pickle.dumps(patch))


class TestParsedMacro(ContractTestCase):
    ContractType = ParsedMacro

    def _ok_dict(self):
        return {
            'name': 'foo',
            'path': '/root/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'macro_sql': '{% macro foo() %}select 1 as id{% endmacro %}',
            'root_path': '/root/',
            'resource_type': 'macro',
            'unique_id': 'macro.test.foo',
            'tags': [],
            'depends_on': {'macros': []},
            'meta': {},
            'description': 'my macro description',
            'docs': {'show': True},
            'arguments': [],
        }

    def test_ok(self):
        macro_dict = self._ok_dict()
        macro = self.ContractType(
            name='foo',
            path='/root/path.sql',
            original_file_path='/root/path.sql',
            package_name='test',
            macro_sql='{% macro foo() %}select 1 as id{% endmacro %}',
            root_path='/root/',
            resource_type=NodeType.Macro,
            unique_id='macro.test.foo',
            tags=[],
            depends_on=MacroDependsOn(),
            meta={},
            description='my macro description',
            arguments=[],
        )
        self.assert_symmetric(macro, macro_dict)
        self.assertEqual(macro.local_vars(), {})
        pickle.loads(pickle.dumps(macro))

    def test_invalid_missing_unique_id(self):
        bad_missing_uid = self._ok_dict()
        del bad_missing_uid['unique_id']
        self.assert_fails_validation(bad_missing_uid)

    def test_invalid_extra_field(self):
        bad_extra_field = self._ok_dict()
        bad_extra_field['extra'] = 'too many fields'
        self.assert_fails_validation(bad_extra_field)


class TestParsedDocumentation(ContractTestCase):
    ContractType = ParsedDocumentation

    def _ok_dict(self):
        return {
            'block_contents': 'some doc contents',
            'name': 'foo',
            'original_file_path': '/root/docs/doc.md',
            'package_name': 'test',
            'path': '/root/docs',
            'root_path': '/root',
            'unique_id': 'test.foo',
        }

    def test_ok(self):
        doc_dict = self._ok_dict()
        doc = self.ContractType(
            package_name='test',
            root_path='/root',
            path='/root/docs',
            original_file_path='/root/docs/doc.md',
            name='foo',
            unique_id='test.foo',
            block_contents='some doc contents'
        )
        self.assert_symmetric(doc, doc_dict)
        pickle.loads(pickle.dumps(doc))

    def test_invalid_missing(self):
        bad_missing_contents = self._ok_dict()
        del bad_missing_contents['block_contents']
        self.assert_fails_validation(bad_missing_contents)

    def test_invalid_extra(self):
        bad_extra_field = self._ok_dict()
        bad_extra_field['extra'] = 'more'
        self.assert_fails_validation(bad_extra_field)


class TestParsedSourceDefinition(ContractTestCase):
    ContractType = ParsedSourceDefinition

    def _minimum_dict(self):
        return {
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
            'columns': {},
            'quoting': {},
            'unique_id': 'test.source.my_source.my_source_table',
            'meta': {},
            'source_meta': {},
            'tags': [],
            'config': {
                'enabled': True,
            }
        }
        source_def = self.ContractType(
            columns={},
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
            tags=[],
            config=SourceConfig(),
        )
        self.assert_symmetric(source_def, source_def_dict)
        minimum = self._minimum_dict()
        self.assert_from_dict(source_def, minimum)
        pickle.loads(pickle.dumps(source_def))

    def test_invalid_missing(self):
        bad_missing_name = self._minimum_dict()
        del bad_missing_name['name']
        self.assert_fails_validation(bad_missing_name)

    def test_invalid_bad_resource_type(self):
        bad_resource_type = self._minimum_dict()
        bad_resource_type['resource_type'] = str(NodeType.Model)
        self.assert_fails_validation(bad_resource_type)
