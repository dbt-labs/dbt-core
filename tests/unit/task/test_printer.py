"""Unit tests for dbt.task.printer.print_run_result_error.

Issue #11350: When ``store_failures=True`` and a test errors due to a
SQL/database compilation error (``NodeStatus.Error``), dbt was still logging
``CheckNodeTestFailure`` ("see test failures table") even though the failures
table was never created. The message should appear when the test actually ran
and stored its failing rows (``Fail`` or ``Warn``) but be suppressed on
``Error``.
"""

from unittest import mock

from dbt.contracts.results import NodeStatus
from dbt.task.printer import print_run_result_error


def _make_result(status: NodeStatus, should_store_failures: bool = True) -> mock.Mock:
    """Build a minimal result mock for print_run_result_error.

    Attributes are set explicitly so ``getattr(result.node, "should_store_failures")``
    returns the real boolean (a bare Mock would auto-create a truthy attribute).
    """
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
    return result


def _fired_event_types(mock_fire: mock.Mock) -> list:
    return [type(call[0][0]).__name__ for call in mock_fire.call_args_list if call[0]]


class TestPrintRunResultErrorStoreFailures:
    """CheckNodeTestFailure must fire only when the failures table was actually
    created. A test that Fails or Warns runs the query and stores the failing
    rows, so the message is useful. A test that Errors never created the table,
    so the message would point users at a non-existent relation."""

    def test_check_node_test_failure_fires_for_fail_status(self):
        """NodeStatus.Fail + should_store_failures → CheckNodeTestFailure fires."""
        result = _make_result(NodeStatus.Fail, should_store_failures=True)
        with mock.patch("dbt.task.printer.fire_event") as mock_fire:
            print_run_result_error(result)
        assert "CheckNodeTestFailure" in _fired_event_types(mock_fire), (
            "Expected CheckNodeTestFailure to fire when status=Fail and "
            "should_store_failures=True"
        )

    def test_check_node_test_failure_fires_for_warn_status(self):
        """NodeStatus.Warn + should_store_failures → CheckNodeTestFailure fires.

        A ``severity: warn`` test still runs its query and writes the failures
        table (store_failures is independent of severity), so users should be
        pointed at the real table. Warn only reaches this code path via
        ``is_warning=True``, mirroring print_run_end_messages.
        """
        result = _make_result(NodeStatus.Warn, should_store_failures=True)
        with mock.patch("dbt.task.printer.fire_event") as mock_fire:
            print_run_result_error(result, is_warning=True)
        assert "CheckNodeTestFailure" in _fired_event_types(mock_fire), (
            "Expected CheckNodeTestFailure to fire when status=Warn and "
            "should_store_failures=True - the failures table is created for "
            "warn-severity tests too"
        )

    def test_check_node_test_failure_suppressed_for_error_status(self):
        """NodeStatus.Error + should_store_failures → CheckNodeTestFailure must NOT fire.

        The failures table was never created when the SQL/DB itself errored, so
        pointing users there would produce a confusing 'table not found' error.
        """
        result = _make_result(NodeStatus.Error, should_store_failures=True)
        with mock.patch("dbt.task.printer.fire_event") as mock_fire:
            print_run_result_error(result)
        assert "CheckNodeTestFailure" not in _fired_event_types(mock_fire), (
            "CheckNodeTestFailure must not fire when status=Error: the failures "
            "table was never created"
        )

    def test_check_node_test_failure_suppressed_when_store_failures_false(self):
        """should_store_failures=False → CheckNodeTestFailure never fires regardless of status."""
        result = _make_result(NodeStatus.Fail, should_store_failures=False)
        with mock.patch("dbt.task.printer.fire_event") as mock_fire:
            print_run_result_error(result)
        assert "CheckNodeTestFailure" not in _fired_event_types(mock_fire)
