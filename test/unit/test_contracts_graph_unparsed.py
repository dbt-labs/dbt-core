import pickle
from datetime import timedelta

from dbt.contracts.graph.unparsed import (
    UnparsedNode, UnparsedRunHook, UnparsedMacro, Time, TimePeriod,
    FreshnessStatus, FreshnessThreshold, Quoting, UnparsedSourceDefinition,
    UnparsedSourceTableDefinition, UnparsedDocumentationFile, NamedTested,
    UnparsedNodeUpdate
)
from dbt.node_types import NodeType
from .utils import ContractTestCase


class TestUnparsedMacro(ContractTestCase):
    ContractType = UnparsedMacro

    def test_ok(self):
        macro_dict = {
            'path': '/root/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': '{% macro foo() %}select 1 as id{% endmacro %}',
            'root_path': '/root/',
            'resource_type': 'macro',
        }
        macro = self.ContractType(
            path='/root/path.sql',
            original_file_path='/root/path.sql',
            package_name='test',
            raw_sql='{% macro foo() %}select 1 as id{% endmacro %}',
            root_path='/root/',
            resource_type=NodeType.Macro,
        )
        self.assert_symmetric(macro, macro_dict)
        pickle.loads(pickle.dumps(macro))

    def test_invalid_missing_field(self):
        macro_dict = {
            'path': '/root/path.sql',
            'original_file_path': '/root/path.sql',
            # 'package_name': 'test',
            'raw_sql': '{% macro foo() %}select 1 as id{% endmacro %}',
            'root_path': '/root/',
            'resource_type': 'macro',
        }
        self.assert_fails_validation(macro_dict)

    def test_invalid_extra_field(self):
        macro_dict = {
            'path': '/root/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': '{% macro foo() %}select 1 as id{% endmacro %}',
            'root_path': '/root/',
            'extra': 'extra',
            'resource_type': 'macro',
        }
        self.assert_fails_validation(macro_dict)


class TestUnparsedNode(ContractTestCase):
    ContractType = UnparsedNode

    def test_ok(self):
        node_dict = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': NodeType.Model,
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from {{ ref("thing") }}',
        }
        node = self.ContractType(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='select * from {{ ref("thing") }}',
            name='foo',
            resource_type=NodeType.Model,
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)

        self.assert_fails_validation(node_dict, cls=UnparsedRunHook)
        self.assert_fails_validation(node_dict, cls=UnparsedMacro)
        pickle.loads(pickle.dumps(node))

    def test_empty(self):
        node_dict = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': NodeType.Model,
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': '  \n',
        }
        node = UnparsedNode(
            package_name='test',
            root_path='/root/',
            path='/root/x/path.sql',
            original_file_path='/root/path.sql',
            raw_sql='  \n',
            name='foo',
            resource_type=NodeType.Model,
        )
        self.assert_symmetric(node, node_dict)
        self.assertTrue(node.empty)

        self.assert_fails_validation(node_dict, cls=UnparsedRunHook)
        self.assert_fails_validation(node_dict, cls=UnparsedMacro)

    def test_bad_type(self):
        node_dict = {
            'name': 'foo',
            'root_path': '/root/',
            'resource_type': NodeType.Source,  # not valid!
            'path': '/root/x/path.sql',
            'original_file_path': '/root/path.sql',
            'package_name': 'test',
            'raw_sql': 'select * from {{ ref("thing") }}',
        }
        self.assert_fails_validation(node_dict)


class TestUnparsedRunHook(ContractTestCase):
    ContractType = UnparsedRunHook

    def test_ok(self):
        node_dict = {
            'name': 'foo',
            'root_path': 'test/dbt_project.yml',
            'resource_type': NodeType.Operation,
            'path': '/root/dbt_project.yml',
            'original_file_path': '/root/dbt_project.yml',
            'package_name': 'test',
            'raw_sql': 'GRANT select on dbt_postgres',
            'index': 4
        }
        node = self.ContractType(
            package_name='test',
            root_path='test/dbt_project.yml',
            path='/root/dbt_project.yml',
            original_file_path='/root/dbt_project.yml',
            raw_sql='GRANT select on dbt_postgres',
            name='foo',
            resource_type=NodeType.Operation,
            index=4,
        )
        self.assert_symmetric(node, node_dict)
        self.assert_fails_validation(node_dict, cls=UnparsedNode)
        pickle.loads(pickle.dumps(node))

    def test_bad_type(self):
        node_dict = {
            'name': 'foo',
            'root_path': 'test/dbt_project.yml',
            'resource_type': NodeType.Model,  # invalid
            'path': '/root/dbt_project.yml',
            'original_file_path': '/root/dbt_project.yml',
            'package_name': 'test',
            'raw_sql': 'GRANT select on dbt_postgres',
            'index': 4
        }
        self.assert_fails_validation(node_dict)


class TestFreshnessThreshold(ContractTestCase):
    ContractType = FreshnessThreshold

    def test_empty(self):
        empty = self.ContractType(None, None)
        self.assert_symmetric(empty, {})
        self.assertEqual(empty.status(float('Inf')), FreshnessStatus.Pass)
        self.assertEqual(empty.status(0), FreshnessStatus.Pass)

    def test_both(self):
        threshold = self.ContractType(
            warn_after=Time(count=18, period=TimePeriod.hour),
            error_after=Time(count=2, period=TimePeriod.day),
        )
        dct = {
            'error_after': {'count': 2, 'period': 'day'},
            'warn_after': {'count': 18, 'period': 'hour'}
        }
        self.assert_symmetric(threshold, dct)

        error_seconds = timedelta(days=3).total_seconds()
        warn_seconds = timedelta(days=1).total_seconds()
        pass_seconds = timedelta(hours=3).total_seconds()
        self.assertEqual(threshold.status(error_seconds), FreshnessStatus.Error)
        self.assertEqual(threshold.status(warn_seconds), FreshnessStatus.Warn)
        self.assertEqual(threshold.status(pass_seconds), FreshnessStatus.Pass)
        pickle.loads(pickle.dumps(threshold))

    def test_merged(self):
        t1 = self.ContractType(
            warn_after=Time(count=36, period=TimePeriod.hour),
            error_after=Time(count=2, period=TimePeriod.day),
        )
        t2 = self.ContractType(
            warn_after=Time(count=18, period=TimePeriod.hour),
        )
        threshold = self.ContractType(
            warn_after=Time(count=18, period=TimePeriod.hour),
            error_after=Time(count=2, period=TimePeriod.day),
        )
        self.assertEqual(threshold, t1.merged(t2))

        error_seconds = timedelta(days=3).total_seconds()
        warn_seconds = timedelta(days=1).total_seconds()
        pass_seconds = timedelta(hours=3).total_seconds()
        self.assertEqual(threshold.status(error_seconds), FreshnessStatus.Error)
        self.assertEqual(threshold.status(warn_seconds), FreshnessStatus.Warn)
        self.assertEqual(threshold.status(pass_seconds), FreshnessStatus.Pass)


class TestQuoting(ContractTestCase):
    ContractType = Quoting

    def test_empty(self):
        empty = self.ContractType()
        self.assert_symmetric(empty, {})

    def test_partial(self):
        a = self.ContractType(None, True, False)
        b = self.ContractType(True, False, None)
        self.assert_symmetric(a, {'schema': True, 'identifier': False})
        self.assert_symmetric(b, {'database': True, 'schema': False})

        c = a.merged(b)
        self.assertEqual(c, self.ContractType(True, False, False))
        self.assert_symmetric(
            c, {'database': True, 'schema': False, 'identifier': False}
        )
        pickle.loads(pickle.dumps(c))


class TestUnparsedSourceDefinition(ContractTestCase):
    ContractType = UnparsedSourceDefinition

    def test_defaults(self):
        minimum = self.ContractType(name='foo')
        from_dict = {'name': 'foo'}
        to_dict = {
            'name': 'foo',
            'description': '',
            'freshness': {},
            'quoting': {},
            'tables': [],
            'loader': '',
            'meta': {},
        }
        self.assert_from_dict(minimum, from_dict)
        self.assert_to_dict(minimum, to_dict)

    def test_contents(self):
        empty = self.ContractType(
            name='foo',
            description='a description',
            quoting=Quoting(database=False),
            loader='some_loader',
            freshness=FreshnessThreshold(),
            tables=[],
            meta={},
        )
        dct = {
            'name': 'foo',
            'description': 'a description',
            'quoting': {'database': False},
            'loader': 'some_loader',
            'freshness': {},
            'tables': [],
            'meta': {},
        }
        self.assert_symmetric(empty, dct)

    def test_table_defaults(self):
        table_1 = UnparsedSourceTableDefinition(name='table1')
        table_2 = UnparsedSourceTableDefinition(
            name='table2',
            description='table 2',
            quoting=Quoting(database=True),
        )
        source = self.ContractType(
            name='foo',
            tables=[table_1, table_2]
        )
        from_dict = {
            'name': 'foo',
            'tables': [
                {'name': 'table1'},
                {
                    'name': 'table2',
                    'description': 'table 2',
                    'quoting': {'database': True},
                },
            ],
        }
        to_dict = {
            'name': 'foo',
            'description': '',
            'loader': '',
            'freshness': {},
            'quoting': {},
            'meta': {},
            'tables': [
                {
                    'name': 'table1',
                    'description': '',
                    'tests': [],
                    'columns': [],
                    'quoting': {},
                    'external': {},
                    'freshness': {},
                    'meta': {},
                },
                {
                    'name': 'table2',
                    'description': 'table 2',
                    'tests': [],
                    'columns': [],
                    'quoting': {'database': True},
                    'external': {},
                    'freshness': {},
                    'meta': {},
                },
            ],
        }
        self.assert_from_dict(source, from_dict)
        self.assert_symmetric(source, to_dict)
        pickle.loads(pickle.dumps(source))


class TestUnparsedDocumentationFile(ContractTestCase):
    ContractType = UnparsedDocumentationFile

    def test_ok(self):
        doc = self.ContractType(
            package_name='test',
            root_path='/root',
            path='/root/docs',
            original_file_path='/root/docs/doc.md',
            file_contents='blah blah blah',
        )
        doc_dict = {
            'package_name': 'test',
            'root_path': '/root',
            'path': '/root/docs',
            'original_file_path': '/root/docs/doc.md',
            'file_contents': 'blah blah blah',
        }
        self.assert_symmetric(doc, doc_dict)
        self.assertEqual(doc.resource_type, NodeType.Documentation)
        self.assert_fails_validation(doc_dict, UnparsedNode)
        pickle.loads(pickle.dumps(doc))

    def test_extra_field(self):
        self.assert_fails_validation({})
        doc_dict = {
            'package_name': 'test',
            'root_path': '/root',
            'path': '/root/docs',
            'original_file_path': '/root/docs/doc.md',
            'file_contents': 'blah blah blah',
            'resource_type': 'docs',
        }
        self.assert_fails_validation(doc_dict)


class TestUnparsedNodeUpdate(ContractTestCase):
    ContractType = UnparsedNodeUpdate

    def test_defaults(self):
        minimum = self.ContractType(
            name='foo',
            yaml_key='models',
            original_file_path='/some/fake/path',
            package_name='test',
        )
        from_dict = {
            'name': 'foo',
            'yaml_key': 'models',
            'original_file_path': '/some/fake/path',
            'package_name': 'test',
        }
        to_dict = {
            'name': 'foo',
            'yaml_key': 'models',
            'original_file_path': '/some/fake/path',
            'package_name': 'test',
            'columns': [],
            'description': '',
            'tests': [],
            'meta': {},
        }
        self.assert_from_dict(minimum, from_dict)
        self.assert_to_dict(minimum, to_dict)

    def test_contents(self):
        update = self.ContractType(
            name='foo',
            yaml_key='models',
            original_file_path='/some/fake/path',
            package_name='test',
            description='a description',
            tests=['table_test'],
            meta={'key': ['value1', 'value2']},
            columns=[
                NamedTested(
                    name='x',
                    description='x description',
                    meta={'key2': 'value3'},
                ),
                NamedTested(
                    name='y',
                    description='y description',
                    tests=[
                        'unique',
                        {'accepted_values': {'values': ['blue', 'green']}}
                    ],
                    meta={},
                ),
            ],
        )
        dct = {
            'name': 'foo',
            'yaml_key': 'models',
            'original_file_path': '/some/fake/path',
            'package_name': 'test',
            'description': 'a description',
            'tests': ['table_test'],
            'meta': {'key': ['value1', 'value2']},
            'columns': [
                {
                    'name': 'x',
                    'description': 'x description',
                    'tests': [],
                    'meta': {'key2': 'value3'},
                },
                {
                    'name': 'y',
                    'description': 'y description',
                    'tests': [
                        'unique',
                        {'accepted_values': {'values': ['blue', 'green']}}
                    ],
                    'meta': {},
                },
            ],
        }
        self.assert_symmetric(update, dct)
        pickle.loads(pickle.dumps(update))

    def test_bad_test_type(self):
        dct = {
            'name': 'foo',
            'yaml_key': 'models',
            'original_file_path': '/some/fake/path',
            'package_name': 'test',
            'description': 'a description',
            'tests': ['table_test'],
            'meta': {'key': ['value1', 'value2']},
            'columns': [
                {
                    'name': 'x',
                    'description': 'x description',
                    'tests': [],
                    'meta': {'key2': 'value3'},
                },
                {
                    'name': 'y',
                    'description': 'y description',
                    'tests': [
                        100,
                        {'accepted_values': {'values': ['blue', 'green']}}
                    ],
                    'meta': {},
                    'yaml_key': 'models',
                    'original_file_path': '/some/fake/path',
                },
            ],
        }
        self.assert_fails_validation(dct)

        dct = {
            'name': 'foo',
            'yaml_key': 'models',
            'original_file_path': '/some/fake/path',
            'package_name': 'test',
            'description': 'a description',
            'tests': ['table_test'],
            'meta': {'key': ['value1', 'value2']},
            'columns': [
                # column missing a name
                {
                    'description': 'x description',
                    'tests': [],
                    'meta': {'key2': 'value3'},
                },
                {
                    'name': 'y',
                    'description': 'y description',
                    'tests': [
                        'unique',
                        {'accepted_values': {'values': ['blue', 'green']}}
                    ],
                    'meta': {},
                    'yaml_key': 'models',
                    'original_file_path': '/some/fake/path',
                },
            ],
        }
        self.assert_fails_validation(dct)

        # missing a name
        dct = {
            'yaml_key': 'models',
            'original_file_path': '/some/fake/path',
            'package_name': 'test',
            'description': 'a description',
            'tests': ['table_test'],
            'meta': {'key': ['value1', 'value2']},
            'columns': [
                {
                    'name': 'x',
                    'description': 'x description',
                    'tests': [],
                    'meta': {'key2': 'value3'},
                },
                {
                    'name': 'y',
                    'description': 'y description',
                    'tests': [
                        'unique',
                        {'accepted_values': {'values': ['blue', 'green']}}
                    ],
                    'meta': {},
                    'yaml_key': 'models',
                    'original_file_path': '/some/fake/path',
                },
            ],
        }
        self.assert_fails_validation(dct)
