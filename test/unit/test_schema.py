import unittest

import decimal
import dbt.schema

class TestStringType(unittest.TestCase):

    def test__character_type(self):
        col = dbt.schema.Column(
            'fieldname',
            'character',
            char_size=10
        )

        self.assertEqual(col.data_type, 'character varying(10)')


class TestNumericType(unittest.TestCase):

    def test__numeric_type(self):
        col = dbt.schema.Column(
            'fieldname',
            'numeric',
            numeric_precision=decimal.Decimal('12'),
            numeric_scale=decimal.Decimal('2'))

        self.assertEqual(col.data_type, 'numeric(12,2)')

    def test__numeric_type_with_no_precision(self):
        # PostgreSQL, at least, will allow empty numeric precision
        col = dbt.schema.Column(
            'fieldname',
            'numeric',
            numeric_precision=None)

        self.assertEqual(col.data_type, 'numeric')
