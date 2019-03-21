import mock
import unittest

from dbt.contracts.graph.parsed import ParsedNode
from dbt.context.common import Var
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
