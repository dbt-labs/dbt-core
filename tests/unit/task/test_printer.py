"""Unit tests for dbt.task.printer.print_run_result_error.

Issue #11350: When store_failures=True and the test fails due to a
SQL/database error (NodeStatus.Error), dbt was logging
CheckNodeTestFailure ("see test failures table"), but the table was never
created. The message should only appear for NodeStatus.Fail.
"""
from unittest import mock

import pytest

from dbt.contracts.results import NodeStatus
from dbt.events.types import CheckNodeTestFailure
from dbt.task.printer import print_run_result_error


def _make_result(status: NodeStatus, should_store_failures: bool = True) -> mock.Mock:
    result = mock.Mock()
    result.status = status
    result.message = "test message"
    result.node.should_store_failures = should_store_failures
    result.node.relation_name = "test_schema.test_failures"
    result.node.node_info = {}
    result.node.compiled_path = None
    result.node.resource_type = "test"
    result.node.name = "my_test"
    result.node.original_file_path = "tests/my_test.sql"
    # Make getattr(result.node, "should_store_failures", None) work correctly.
    result.node.__class__ = type("MockNode", (), {"should_store_failures": should_store_failures})
    return result


def _fired_event_types(mock_fire: mock.Mock) -> list:
    return [type(call[0][0]).__name__ for call in mock_fire.call_args_list if call[0]]


class TestPrintRunResultErrorStoreFailures:
    """CheckNodeTestFailure must only fire when the test actually ran and
    produced failures — not when the run itself errored (e.g. SQL compile
    error, DB error)."""

    def test_check_node_test_failure_fires_for_fail_status(self):
        """NodeStatus.Fail + should_store_failures → CheckNodeTestFailure fires."""
        result = _make_result(NodeStatus.Fail, should_store_failures=True)
        with mock.patch("dbt.task.printer.fire_event") as mock_fire:
            print_run_result_error(result)
        assert "CheckNodeTestFailure" in _fired_event_types(mock_fire), (
            "Expected CheckNodeTestFailure to fire when status=Fail and should_store_failures=True"
        )

    def test_check_node_test_failure_suppressed_for_error_status(self):
        """NodeStatus.Error + should_store_failures → CheckNodeTestFailure must NOT fire.

        The failures table was never created when the SQL/DB itself errored,
        so pointing users there would produce a confusing 'table not found' error.
        """
        result = _make_result(NodeStatus.Error, should_store_failures=True)
        with mock.patch("dbt.task.printer.fire_event") as mock_fire:
            print_run_result_error(result)
        assert "CheckNodeTestFailure" not in _fired_event_types(mock_fire), (
            "CheckNodeTestFailure must not fire when status=Error — "
            "the failures table was never created"
        )

    def test_check_node_test_failure_suppressed_when_store_failures_false(self):
        """should_store_failures=False → CheckNodeTestFailure never fires regardless of status."""
        result = _make_result(NodeStatus.Fail, should_store_failures=False)
        with mock.patch("dbt.task.printer.fire_event") as mock_fire:
            print_run_result_error(result)
        assert "CheckNodeTestFailure" not in _fired_event_types(mock_fire)
