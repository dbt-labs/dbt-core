from decimal import Decimal
import unittest
import os

import dbt.flags
from dbt.compat import bigint
from dbt.task import generate


class GenerateTest(unittest.TestCase):
    def setUp(self):
        dbt.flags.STRICT_MODE = True
        self.maxDiff = None

    def test__unflatten_empty(self):
        columns = []
        expected = {}
        result = generate.unflatten(columns)
        self.assertEqual(result, expected)

    def test__unflatten_one_column(self):
        columns = [{
            'column_comment': None,
            'column_index': Decimal('1'),
            'column_name': 'id',
            'column_type': 'integer',
            'table_comment': None,
            'table_name': 'test_table',
            'table_schema': 'test_schema',
            'table_type': 'BASE TABLE'
        }]

        expected = {
            'test_schema': {
                'test_table': {
                    'metadata': {
                        'comment': None,
                        'name': 'test_table',
                        'type': 'BASE TABLE',
                        'schema': 'test_schema',
                    },
                    'columns': [
                        {
                            'type': 'integer',
                            'comment': None,
                            'index': bigint(1),
                            'name': 'id'
                        }
                    ]
                }
            }
        }
        result = generate.unflatten(columns)
        self.assertEqual(result, expected)

    def test__unflatten_multiple_schemas(self):
        columns = [
            {
                'column_comment': None,
                'column_index': Decimal('1'),
                'column_name': 'id',
                'column_type': 'integer',
                'table_comment': None,
                'table_name': 'test_table',
                'table_schema': 'test_schema',
                'table_type': 'BASE TABLE'
            },
            {
                'column_comment': None,
                'column_index': Decimal('2'),
                'column_name': 'name',
                'column_type': 'text',
                'table_comment': None,
                'table_name': 'test_table',
                'table_schema': 'test_schema',
                'table_type': 'BASE TABLE'
            },
            {
                'column_comment': None,
                'column_index': Decimal('1'),
                'column_name': 'id',
                'column_type': 'integer',
                'table_comment': None,
                'table_name': 'other_test_table',
                'table_schema': 'test_schema',
                'table_type': 'BASE TABLE',
            },
            {
                'column_comment': None,
                'column_index': Decimal('2'),
                'column_name': 'email',
                'column_type': 'character varying',
                'table_comment': None,
                'table_name': 'other_test_table',
                'table_schema': 'test_schema',
                'table_type': 'BASE TABLE',
            },
            {
                'column_comment': None,
                'column_index': Decimal('1'),
                'column_name': 'id',
                'column_type': 'integer',
                'table_comment': None,
                'table_name': 'test_table',
                'table_schema': 'other_test_schema',
                'table_type': 'BASE TABLE'
            },
            {
                'column_comment': None,
                'column_index': Decimal('2'),
                'column_name': 'name',
                'column_type': 'text',
                'table_comment': None,
                'table_name': 'test_table',
                'table_schema': 'other_test_schema',
                'table_type': 'BASE TABLE'
            },
        ]

        expected = {
            'test_schema': {
                'test_table': {
                    'metadata': {
                        'comment': None,
                        'name': 'test_table',
                        'type': 'BASE TABLE',
                        'schema': 'test_schema',
                    },
                    'columns': [
                        {
                            'type': 'integer',
                            'comment': None,
                            'index': bigint(1),
                            'name': 'id'
                        },
                        {
                            'type': 'text',
                            'comment': None,
                            'index': Decimal('2'),
                            'name': 'name',
                        }
                    ],
                },
                'other_test_table': {
                    'metadata': {
                        'comment': None,
                        'name': 'other_test_table',
                        'type': 'BASE TABLE',
                        'schema': 'test_schema',
                    },
                    'columns': [
                        {
                            'type': 'integer',
                            'comment': None,
                            'index': bigint(1),
                            'name': 'id'
                        },
                        {
                            'type': 'character varying',
                            'comment': None,
                            'index': Decimal('2'),
                            'name': 'email',
                        }
                    ]
                }
            },
            'other_test_schema': {
                'test_table': {
                    'metadata': {
                        'comment': None,
                        'name': 'test_table',
                        'type': 'BASE TABLE',
                        'schema': 'other_test_schema',
                    },
                    'columns': [
                        {
                            'type': 'integer',
                            'comment': None,
                            'index': bigint(1),
                            'name': 'id'
                        },
                        {
                            'type': 'text',
                            'comment': None,
                            'index': Decimal('2'),
                            'name': 'name',
                        }
                    ],
                },
            }
        }
        result = generate.unflatten(columns)
        self.assertEqual(result, expected)
