import mock
import unittest

from dbt.contracts.graph.parsed import ParsedNode
from dbt.context.common import Var
from dbt.context import parser, runtime
import dbt.exceptions
from test.unit.mock_adapter import adapter_factory


class TestVar(unittest.TestCase):
    def setUp(self):
        self.model = ParsedNode(
            alias='model_one',
            name='model_one',
            database='dbt',
            schema='analytics',
            resource_type='model',
            unique_id='model.root.model_one',
            fqn=['root', 'model_one'],
            empty=False,
            package_name='root',
            original_file_path='model_one.sql',
            root_path='/usr/src/app',
            refs=[],
            sources=[],
            depends_on={
                'nodes': [],
                'macros': []
            },
            config={
                'enabled': True,
                'materialized': 'view',
                'post-hook': [],
                'pre-hook': [],
                'vars': {},
                'quoting': {},
                'column_types': {},
                'tags': [],
            },
            tags=[],
            path='model_one.sql',
            raw_sql='',
            description='',
            columns={}
        )
        self.context = mock.MagicMock()

    def test_var_not_none_is_none(self):
        var = Var(self.model, self.context, overrides={'foo': None})
        var.assert_var_defined('foo', None)
        with self.assertRaises(dbt.exceptions.CompilationException):
            var.assert_var_not_none('foo')

    def test_var_defined_is_missing(self):
        var = Var(self.model, self.context, overrides={})
        var.assert_var_defined('foo', 'bar')
        with self.assertRaises(dbt.exceptions.CompilationException):
            var.assert_var_defined('foo', None)


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
