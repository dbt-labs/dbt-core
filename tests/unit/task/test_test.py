import agate
import pytest

from dbt.task.test import list_rows_from_table


class TestListRowsFromTable:
    @pytest.mark.parametrize(
        "agate_table_cols,agate_table_rows,expected_list_rows",
        [
            (["a", "b", "c"], [], [["a", "b", "c"]]),  # no rows
            (["a", "b", "c"], [[1, 2, 3]], [["a", "b", "c"], [1, 2, 3]]),  # single row, no nulls
            (
                ["a", "b", "c"],
                [[1, 2, 3], [2, 3, 4]],
                [["a", "b", "c"], [1, 2, 3], [2, 3, 4]],
            ),  # multiple rows
            (
                ["a", "b", "c"],
                [[None, 2, 3], [2, None, 4]],
                [["a", "b", "c"], [None, 2, 3], [2, None, 4]],
            ),  # multiple rows, with nulls
        ],
    )
    def test_list_rows_from_table_no_sort(
        self, agate_table_cols, agate_table_rows, expected_list_rows
    ):
        table = agate.Table(rows=agate_table_rows, column_names=agate_table_cols)

        list_rows = list_rows_from_table(table)
        assert list_rows == expected_list_rows

    @pytest.mark.parametrize(
        "agate_table_cols,agate_table_rows,expected_list_rows",
        [
            (["a", "b", "c"], [], [["a", "b", "c"]]),  # no rows
            (["a", "b", "c"], [[1, 2, 3]], [["a", "b", "c"], [1, 2, 3]]),  # single row, no nulls
            (
                ["a", "b", "c"],
                [[1, 2, 3], [2, 3, 4]],
                [["a", "b", "c"], [1, 2, 3], [2, 3, 4]],
            ),  # multiple rows, in order
            (
                ["a", "b", "c"],
                [[2, 3, 4], [1, 2, 3]],
                [["a", "b", "c"], [1, 2, 3], [2, 3, 4]],
            ),  # multiple rows, out of order
            (
                ["a", "b", "c"],
                [[None, 2, 3], [2, 3, 4]],
                [["a", "b", "c"], [2, 3, 4], [None, 2, 3]],
            ),  # multiple rows, out of order with nulls in first position
            (
                ["a", "b", "c"],
                [[4, 5, 6], [1, None, 3]],
                [["a", "b", "c"], [1, None, 3], [4, 5, 6]],
            ),  # multiple rows, out of order with null in non-first position
            (
                ["a", "b", "c"],
                [[None, 5, 6], [1, None, 3]],
                [["a", "b", "c"], [1, None, 3], [None, 5, 6]],
            ),  # multiple rows, out of order with nulls in many positions
        ],
    )
    def test_list_rows_from_table_with_sort(
        self, agate_table_cols, agate_table_rows, expected_list_rows
    ):
        table = agate.Table(rows=agate_table_rows, column_names=agate_table_cols)

        list_rows = list_rows_from_table(table, sort=True)
        assert list_rows == expected_list_rows
