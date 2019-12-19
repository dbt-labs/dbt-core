import pickle

from dbt.contracts.graph.compiled import (
    CompiledModelNode, InjectedCTE, CompiledTestNode
)
from dbt.contracts.graph.parsed import (
    DependsOn, NodeConfig, TestConfig
)
from dbt.node_types import NodeType

from .utils import ContractTestCase


class TestCompiledModelNode(ContractTestCase):
    ContractType = CompiledModelNode

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
            'compiled': True,
            'compiled_sql': 'select * from whatever',
            'extra_ctes': [{'id': 'whatever', 'sql': 'select * from other'}],
            'extra_ctes_injected': True,
            'injected_sql': 'with whatever as (select * from other) select * from whatever',
            'wrapped_sql': 'None',
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
            wrapped_sql='None',
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)
        self.assertTrue(node.is_refable)
        self.assertFalse(node.is_ephemeral)
        self.assertEqual(node.local_vars(), {})

    def test_invalid_extra_fields(self):
        bad_extra = {
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
            'notvalid': 'nope',
        }
        self.assert_fails_validation(bad_extra)

    def test_invalid_bad_type(self):
        bad_type = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': str(NodeType.Macro),
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
        self.assert_fails_validation(bad_type)


class TestCompiledTestNode(ContractTestCase):
    ContractType = CompiledTestNode

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
                'severity': 'error',
            },
            'docrefs': [],
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
            resource_type=NodeType.Test,
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
            config=TestConfig(),
            meta={},
            compiled=False,
            extra_ctes=[],
            extra_ctes_injected=False,
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)
        self.assertFalse(node.is_refable)
        self.assertFalse(node.is_ephemeral)
        self.assertEqual(node.local_vars(), {})

        minimum = {
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
        }
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
            'docrefs': [],
            'columns': {},
            'meta': {},
            'compiled': True,
            'compiled_sql': 'select * from whatever',
            'extra_ctes': [{'id': 'whatever', 'sql': 'select * from other'}],
            'extra_ctes_injected': True,
            'injected_sql': 'with whatever as (select * from other) select * from whatever',
            'wrapped_sql': 'select count(*) from (with whatever as (select * from other) select * from whatever) sbq',
            'column_name': 'id',
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
            wrapped_sql='select count(*) from (with whatever as (select * from other) select * from whatever) sbq',
            column_name='id',
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)
        self.assertFalse(node.is_refable)
        self.assertFalse(node.is_ephemeral)
        self.assertEqual(node.local_vars(), {})

    def test_invalid_extra_fields(self):
        bad_extra = {
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
            'extra': 'extra value',
        }
        self.assert_fails_validation(bad_extra)

    def test_invalid_resource_type(self):
        bad_type = {
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
        self.assert_fails_validation(bad_type)
