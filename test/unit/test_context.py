import mock
import unittest

from dbt.contracts.graph.parsed import ParsedNode
from dbt.context.common import Var
from dbt.context.parser import Var as ParserVar
import dbt.exceptions


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

    def test_var_default_something(self):
        var = Var(self.model, self.context, overrides={'foo': 'baz'})
        self.assertEqual(var('foo'), 'baz')
        self.assertEqual(var('foo', 'bar'), 'baz')

    def test_var_default_none(self):
        var = Var(self.model, self.context, overrides={'foo': None})
        self.assertEqual(var('foo'), None)
        self.assertEqual(var('foo', 'bar'), None)

    def test_var_not_defined(self):
        var = Var(self.model, self.context, overrides={})

        self.assertEqual(var('foo', 'bar'), 'bar')
        with self.assertRaises(dbt.exceptions.CompilationException):
            var('foo')

    def test_parser_var_default_something(self):
        var = ParserVar(self.model, self.context, overrides={'foo': 'baz'})
        self.assertEqual(var('foo'), 'baz')
        self.assertEqual(var('foo', 'bar'), 'baz')

    def test_parser_var_default_none(self):
        var = ParserVar(self.model, self.context, overrides={'foo': None})
        self.assertEqual(var('foo'), None)
        self.assertEqual(var('foo', 'bar'), None)

    def test_parser_var_not_defined(self):
        # at parse-time, we should not raise if we encounter a missing var
        # that way disabled models don't get parse errors
        var = ParserVar(self.model, self.context, overrides={})

        self.assertEqual(var('foo', 'bar'), 'bar')
        self.assertEqual(var('foo'), None)
