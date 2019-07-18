import unittest
from unittest import mock

from dbt.contracts.graph.parsed import ParsedModelNode, NodeConfig, DependsOn
from dbt.context import parser, runtime
from dbt.node_types import NodeType
import dbt.exceptions
from .mock_adapter import adapter_factory


class TestVar(unittest.TestCase):
    def setUp(self):
        self.model = ParsedModelNode(
            alias='model_one',
            name='model_one',
            database='dbt',
            schema='analytics',
            resource_type=NodeType.Model,
            unique_id='model.root.model_one',
            fqn=['root', 'model_one'],
            package_name='root',
            original_file_path='model_one.sql',
            root_path='/usr/src/app',
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            config=NodeConfig.from_dict({
                'enabled': True,
                'materialized': 'view',
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'vars': {},
                'quoting': {},
                'column_types': {},
                'tags': [],
            }),
            tags=[],
            path='model_one.sql',
            raw_sql='',
            description='',
            columns={}
        )
        self.context = mock.MagicMock()

    def test_var_default_something(self):
        var = runtime.Var(self.model, self.context, overrides={'foo': 'baz'})
        self.assertEqual(var('foo'), 'baz')
        self.assertEqual(var('foo', 'bar'), 'baz')

    def test_var_default_none(self):
        var = runtime.Var(self.model, self.context, overrides={'foo': None})
        self.assertEqual(var('foo'), None)
        self.assertEqual(var('foo', 'bar'), None)

    def test_var_not_defined(self):
        var = runtime.Var(self.model, self.context, overrides={})

        self.assertEqual(var('foo', 'bar'), 'bar')
        with self.assertRaises(dbt.exceptions.CompilationException):
            var('foo')

    def test_parser_var_default_something(self):
        var = parser.Var(self.model, self.context, overrides={'foo': 'baz'})
        self.assertEqual(var('foo'), 'baz')
        self.assertEqual(var('foo', 'bar'), 'baz')

    def test_parser_var_default_none(self):
        var = parser.Var(self.model, self.context, overrides={'foo': None})
        self.assertEqual(var('foo'), None)
        self.assertEqual(var('foo', 'bar'), None)

    def test_parser_var_not_defined(self):
        # at parse-time, we should not raise if we encounter a missing var
        # that way disabled models don't get parse errors
        var = parser.Var(self.model, self.context, overrides={})

        self.assertEqual(var('foo', 'bar'), 'bar')
        self.assertEqual(var('foo'), None)


class TestParseWrapper(unittest.TestCase):
    def setUp(self):
        self.mock_config = mock.MagicMock()
        adapter_class = adapter_factory()
        self.mock_adapter = adapter_class(self.mock_config)
        self.wrapper = parser.DatabaseWrapper(self.mock_adapter)
        self.responder = self.mock_adapter.responder

    def test_unwrapped_method(self):
        self.assertEqual(self.wrapper.quote('test_value'), '"test_value"')
        self.responder.quote.assert_called_once_with('test_value')

    def test_wrapped_method(self):
        found = self.wrapper.get_relation('database', 'schema', 'identifier')
        self.assertEqual(found, None)
        self.responder.get_relation.assert_not_called()


class TestRuntimeWrapper(unittest.TestCase):
    def setUp(self):
        self.mock_config = mock.MagicMock()
        self.mock_config.quoting = {'database': True, 'schema': True, 'identifier': True}
        adapter_class = adapter_factory()
        self.mock_adapter = adapter_class(self.mock_config)
        self.wrapper = runtime.DatabaseWrapper(self.mock_adapter)
        self.responder = self.mock_adapter.responder

    def test_unwrapped_method(self):
        # the 'quote' method isn't wrapped, we should get our expected inputs
        self.assertEqual(self.wrapper.quote('test_value'), '"test_value"')
        self.responder.quote.assert_called_once_with('test_value')

    def test_wrapped_method(self):
        rel = mock.MagicMock()
        rel.matches.return_value = True
        self.responder.list_relations_without_caching.return_value = [rel]

        found = self.wrapper.get_relation('database', 'schema', 'identifier')

        self.assertEqual(found, rel)
        # it gets called with an information schema relation as the first arg,
        # which is hard to mock.
        self.responder.list_relations_without_caching.assert_called_once_with(
            mock.ANY, 'schema'
        )
