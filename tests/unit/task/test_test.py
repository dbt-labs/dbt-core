from unittest.mock import MagicMock, patch

import agate
import pytest

from dbt.artifacts.schemas.results import TestStatus
from dbt.task.test import TestResultData, TestRunner, list_rows_from_table


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


def _make_test_node(severity="ERROR", error_if="!= 0", warn_if="!= 0"):
    node = MagicMock()
    node.config.severity = severity
    node.config.error_if = error_if
    node.config.warn_if = warn_if
    return node


def _make_runner():
    runner = TestRunner.__new__(TestRunner)
    return runner


class TestBuildTestRunResult:
    @pytest.mark.parametrize(
        "failures,should_error,should_warn,severity,expected_status,expected_failures",
        [
            # The bug fix: passing test with failures > 0 must preserve the count
            (4, False, False, "ERROR", TestStatus.Pass, 4),
            # Simple pass with 0 failures
            (0, False, False, "ERROR", TestStatus.Pass, 0),
            # Error path
            (3, True, False, "ERROR", TestStatus.Fail, 3),
            # Warn path (severity ERROR but should_warn triggers)
            (2, False, True, "ERROR", TestStatus.Warn, 2),
            # Warn severity
            (5, False, True, "WARN", TestStatus.Warn, 5),
        ],
    )
    @patch("dbt.task.test.get_flags")
    def test_failures_always_preserved(
        self,
        mock_get_flags,
        failures,
        should_error,
        should_warn,
        severity,
        expected_status,
        expected_failures,
    ):
        mock_get_flags.return_value.WARN_ERROR = False
        mock_get_flags.return_value.WARN_ERROR_OPTIONS.includes.return_value = False

        node = _make_test_node(severity=severity)
        result = TestResultData(
            failures=failures,
            should_error=should_error,
            should_warn=should_warn,
            adapter_response={},
        )

        run_result = _make_runner().build_test_run_result(node, result)

        assert run_result.status == expected_status
        assert run_result.failures == expected_failures

    @patch("dbt.task.test.get_flags")
    def test_warn_escalated_to_fail_with_warn_error(self, mock_get_flags):
        mock_get_flags.return_value.WARN_ERROR = True
        mock_get_flags.return_value.WARN_ERROR_OPTIONS.includes.return_value = False

        node = _make_test_node(severity="ERROR")
        result = TestResultData(
            failures=7,
            should_error=False,
            should_warn=True,
            adapter_response={},
        )

        run_result = _make_runner().build_test_run_result(node, result)

        assert run_result.status == TestStatus.Fail
        assert run_result.failures == 7
