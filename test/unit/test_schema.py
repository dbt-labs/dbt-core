import unittest

import dbt.schema


class TestNumericType(unittest.TestCase):

    def test__numeric_type(self):
        col = dbt.schema.Column(
            'fieldname',
            'numeric',
            numeric_size='12,2')

        self.assertEqual(col.data_type, 'numeric(12,2)')

    def test__numeric_type_with_no_precision(self):
        # PostgreSQL, at least, will allow empty numeric precision
        col = dbt.schema.Column(
            'fieldname',
            'numeric',
            numeric_size=None)

        self.assertEqual(col.data_type, 'numeric')
