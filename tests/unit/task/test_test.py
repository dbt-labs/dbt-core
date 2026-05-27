import agate
import pytest

from dbt.adapters.postgres import PostgresAdapter
from dbt.artifacts.schemas.results import RunStatus, TestStatus
from dbt.artifacts.schemas.run import RunResult
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.graph.nodes import GenericTestNode
from dbt.events.types import LogTestResult
from dbt.task.test import TestRunner
from dbt_common.events.event_catcher import EventCatcher
from dbt_common.events.event_manager_client import add_callback_to_manager

from dbt.task.test import list_rows_from_table


class TestTestRunnerPrintResultLine:
    @pytest.fixture
    def log_test_result_catcher(self) -> EventCatcher:
        catcher = EventCatcher(event_to_catch=LogTestResult)
        add_callback_to_manager(catcher.catch)
        return catcher

    @pytest.fixture
    def test_runner(
        self,
        postgres_adapter: PostgresAdapter,
        table_id_not_null: GenericTestNode,
        runtime_config: RuntimeConfig,
    ) -> TestRunner:
        return TestRunner(
            config=runtime_config,
            adapter=postgres_adapter,
            node=table_id_not_null,
            node_index=1,
            num_nodes=3,
        )

    @pytest.fixture
    def run_result(self, table_id_not_null: GenericTestNode) -> RunResult:
        return RunResult(
            status=TestStatus.Pass,
            timing=[],
            thread_id="an_id",
            execution_time=0.5,
            adapter_response={"_message": "SELECT (1 rows, 1.2 GB processed)", "code": "SELECT"},
            message=None,
            failures=0,
            batch_results=None,
            node=table_id_not_null,
        )

    def test_print_result_line_includes_adapter_response(
        self,
        log_test_result_catcher: EventCatcher,
        test_runner: TestRunner,
        run_result: RunResult,
    ) -> None:
        test_runner.print_result_line(run_result)
        assert len(log_test_result_catcher.caught_events) == 1
        event = log_test_result_catcher.caught_events[0]
        # Parenthesized content should be in name (for structured logs)
        assert "(1 rows, 1.2 GB processed)" in event.data.name
        # "SELECT" prefix should NOT be in name
        assert "SELECT" not in event.data.name

    def test_print_result_line_empty_adapter_response(
        self,
        log_test_result_catcher: EventCatcher,
        test_runner: TestRunner,
        run_result: RunResult,
    ) -> None:
        run_result.adapter_response = {}
        test_runner.print_result_line(run_result)
        assert len(log_test_result_catcher.caught_events) == 1
        event = log_test_result_catcher.caught_events[0]
        assert "(" not in event.data.name

    def test_print_result_line_adapter_response_missing_message(
        self,
        log_test_result_catcher: EventCatcher,
        test_runner: TestRunner,
        run_result: RunResult,
    ) -> None:
        run_result.adapter_response = {"code": "SELECT"}
        test_runner.print_result_line(run_result)
        assert len(log_test_result_catcher.caught_events) == 1
        event = log_test_result_catcher.caught_events[0]
        assert "(" not in event.data.name


class TestLogTestResultMessage:
    """Test that LogTestResult.message() formats adapter info inside brackets."""

    def test_message_with_adapter_info_in_brackets(self) -> None:
        """When name contains processed info, it should appear inside brackets with status."""
        event = LogTestResult(
            name="my_test (1 rows, 1.2 GB processed)",
            status="pass",
            index=1,
            num_models=5,
            execution_time=1.0,
            num_failures=0,
        )
        msg = event.message()
        # Bytes info should be in brackets (right side), not in the left side message
        assert "PASS my_test" in msg
        assert "PASS (1 rows, 1.2 GB processed)" in msg

    def test_message_without_adapter_info(self) -> None:
        """When name has no adapter info, output should be unchanged."""
        event = LogTestResult(
            name="my_test",
            status="pass",
            index=1,
            num_models=5,
            execution_time=1.0,
            num_failures=0,
        )
        msg = event.message()
        assert "PASS my_test" in msg
        assert "processed" not in msg

    def test_message_fail_with_adapter_info(self) -> None:
        """FAIL status should also include adapter info in brackets."""
        event = LogTestResult(
            name="my_test (1 rows, 500.0 MiB processed)",
            status="fail",
            index=1,
            num_models=5,
            execution_time=2.0,
            num_failures=3,
        )
        msg = event.message()
        assert "FAIL 3 my_test" in msg
        assert "FAIL 3 (1 rows, 500.0 MiB processed)" in msg
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
