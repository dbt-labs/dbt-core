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


def _run_build_test(result, node=None, warn_error=False):
    with patch("dbt.task.test.get_flags") as mock_get_flags:
        mock_get_flags.return_value.WARN_ERROR = warn_error
        mock_get_flags.return_value.WARN_ERROR_OPTIONS.includes.return_value = False
        return _make_runner().build_test_run_result(node or _make_test_node(), result)


def _make_result(failures, should_error, should_warn):
    return TestResultData(
        failures=failures,
        should_error=should_error,
        should_warn=should_warn,
        adapter_response={},
    )


class TestBuildTestRunResult:
    @pytest.mark.parametrize(
        "failures,should_error,should_warn,expected_status",
        [
            (4, False, False, TestStatus.Pass),
            (0, False, False, TestStatus.Pass),
            (3, True, False, TestStatus.Fail),
            (2, False, True, TestStatus.Warn),
        ],
    )
    def test_failures_always_preserved(
        self, failures, should_error, should_warn, expected_status
    ):
        run_result = _run_build_test(_make_result(failures, should_error, should_warn))
        assert run_result.status == expected_status
        assert run_result.failures == failures

    def test_failures_preserved_with_warn_severity(self):
        run_result = _run_build_test(
            _make_result(5, False, True), node=_make_test_node(severity="WARN")
        )
        assert run_result.status == TestStatus.Warn
        assert run_result.failures == 5

    def test_warn_escalated_to_fail_with_warn_error(self):
        run_result = _run_build_test(_make_result(7, False, True), warn_error=True)
        assert run_result.status == TestStatus.Fail
        assert run_result.failures == 7
