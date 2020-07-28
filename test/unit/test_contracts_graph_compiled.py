import pickle

from dbt.contracts.graph.compiled import (
    CompiledModelNode, InjectedCTE, CompiledSchemaTestNode
)
from dbt.contracts.graph.parsed import (
    DependsOn, NodeConfig, TestConfig, TestMetadata
)
from dbt.node_types import NodeType

from .utils import ContractTestCase


class TestCompiledModelNode(ContractTestCase):
    ContractType = CompiledModelNode

    def _minimum(self):
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
            'database': 'test_db',
            'schema': 'test_schema',
            'alias': 'bar',
            'compiled': False,
        }

    def test_basic_uncompiled(self):
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
            'compiled': False,
            'extra_ctes': [],
            'extra_ctes_injected': False,
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
            deferred=False,
            description='',
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=[],
            config=NodeConfig(),
            meta={},
            compiled=False,
            extra_ctes=[],
            extra_ctes_injected=False,
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)
        self.assertTrue(node.is_refable)
        self.assertFalse(node.is_ephemeral)
        self.assertEqual(node.local_vars(), {})

        minimum = self._minimum()
        self.assert_from_dict(node, minimum)
        pickle.loads(pickle.dumps(node))

    def test_basic_compiled(self):
        node_dict = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Model),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from {{ ref("other") }}',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'refs': [],
            'sources': [],
            'depends_on': {'macros': [], 'nodes': []},
            'database': 'test_db',
            'deferred': True,
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
            'compiled': True,
            'compiled_sql': 'select * from whatever',
            'extra_ctes': [{'id': 'whatever', 'sql': 'select * from other'}],
            'extra_ctes_injected': True,
            'injected_sql': 'with whatever as (select * from other) select * from whatever',
        }
        node = self.ContractType(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='select * from {{ ref("other") }}',
            name='foo',
            resource_type=NodeType.Model,
            unique_id='model.test.foo',
            fqn=['test', 'models', 'foo'],
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            deferred=True,
            description='',
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=[],
            config=NodeConfig(),
            meta={},
            compiled=True,
            compiled_sql='select * from whatever',
            extra_ctes=[InjectedCTE('whatever', 'select * from other')],
            extra_ctes_injected=True,
            injected_sql='with whatever as (select * from other) select * from whatever',
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)
        self.assertTrue(node.is_refable)
        self.assertFalse(node.is_ephemeral)
        self.assertEqual(node.local_vars(), {})

    def test_invalid_extra_fields(self):
        bad_extra = self._minimum()
        bad_extra['notvalid'] = 'nope'
        self.assert_fails_validation(bad_extra)

    def test_invalid_bad_type(self):
        bad_type = self._minimum()
        bad_type['resource_type'] = str(NodeType.Macro)
        self.assert_fails_validation(bad_type)


class TestCompiledSchemaTestNode(ContractTestCase):
    ContractType = CompiledSchemaTestNode

    def _minimum(self):
        return {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Test),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from wherever',
            'unique_id': 'model.test.foo',
            'fqn': ['test', 'models', 'foo'],
            'database': 'test_db',
            'schema': 'test_schema',
            'alias': 'bar',
            'test_metadata': {
                'name': 'foo',
                'kwargs': {},
            },
            'compiled': False,
        }

    def test_basic_uncompiled(self):
        node_dict = {
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
                'severity': 'ERROR',
            },
            'deferred': False,
            'docs': {'show': True},
            'columns': {},
            'meta': {},
            'compiled': False,
            'extra_ctes': [],
            'extra_ctes_injected': False,
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
            unique_id='model.test.foo',
            fqn=['test', 'models', 'foo'],
            refs=[],
            sources=[],
            deferred=False,
            depends_on=DependsOn(),
            description='',
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=[],
            config=TestConfig(),
            meta={},
            compiled=False,
            extra_ctes=[],
            extra_ctes_injected=False,
            test_metadata=TestMetadata(namespace=None, name='foo', kwargs={}),
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)
        self.assertFalse(node.is_refable)
        self.assertFalse(node.is_ephemeral)
        self.assertEqual(node.local_vars(), {})

        minimum = self._minimum()
        self.assert_from_dict(node, minimum)
        pickle.loads(pickle.dumps(node))

    def test_basic_compiled(self):
        node_dict = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Test),
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from {{ ref("other") }}',
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
                'materialized': 'view',
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'quoting': {},
                'tags': [],
                'vars': {},
                'severity': 'warn',
            },

            'docs': {'show': True},
            'columns': {},
            'meta': {},
            'compiled': True,
            'compiled_sql': 'select * from whatever',
            'extra_ctes': [{'id': 'whatever', 'sql': 'select * from other'}],
            'extra_ctes_injected': True,
            'injected_sql': 'with whatever as (select * from other) select * from whatever',
            'column_name': 'id',
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
            raw_sql='select * from {{ ref("other") }}',
            name='foo',
            resource_type=NodeType.Test,
            unique_id='model.test.foo',
            fqn=['test', 'models', 'foo'],
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            deferred=False,
            description='',
            database='test_db',
            schema='test_schema',
            alias='bar',
            tags=[],
            config=TestConfig(severity='warn'),
            meta={},
            compiled=True,
            compiled_sql='select * from whatever',
            extra_ctes=[InjectedCTE('whatever', 'select * from other')],
            extra_ctes_injected=True,
            injected_sql='with whatever as (select * from other) select * from whatever',
            column_name='id',
            test_metadata=TestMetadata(namespace=None, name='foo', kwargs={}),
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)
        self.assertFalse(node.is_refable)
        self.assertFalse(node.is_ephemeral)
        self.assertEqual(node.local_vars(), {})

    def test_invalid_extra_fields(self):
        bad_extra = self._minimum()
        bad_extra['extra'] = 'extra value'
        self.assert_fails_validation(bad_extra)

    def test_invalid_resource_type(self):
        bad_type = self._minimum()
        bad_type['resource_type'] = str(NodeType.Model)
        self.assert_fails_validation(bad_type)
