import unittest
from dbt.adapters.sql.connections import SQLConnectionManager

class TestProcessSQLResult(unittest.TestCase):
	def test_duplicated_columns(self):
		cols_with_one_dupe = ['a', 'b', 'a', 'd']
		rows = [(1, 2, 3, 4)]
		self.assertEqual(
			SQLConnectionManager.process_results(cols_with_one_dupe, rows),
			[{"a": 1, "b": 2, "a_2": 3, "d": 4}]
		)

		cols_with_more_dupes = ['a', 'a', 'a', 'b']
		rows = [(1, 2, 3, 4)]
		self.assertEqual(
			SQLConnectionManager.process_results(cols_with_more_dupes, rows),
			[{"a": 1, "a_2": 2, "a_3": 3, "b": 4}]
		)
