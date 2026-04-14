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
        name = log_test_result_catcher.caught_events[0].data.name
        assert "(SELECT (1 rows, 1.2 GB processed))" in name

    def test_print_result_line_empty_adapter_response(
        self,
        log_test_result_catcher: EventCatcher,
        test_runner: TestRunner,
        run_result: RunResult,
    ) -> None:
        run_result.adapter_response = {}
        test_runner.print_result_line(run_result)
        assert len(log_test_result_catcher.caught_events) == 1
        name = log_test_result_catcher.caught_events[0].data.name
        assert "(" not in name

    def test_print_result_line_adapter_response_missing_message(
        self,
        log_test_result_catcher: EventCatcher,
        test_runner: TestRunner,
        run_result: RunResult,
    ) -> None:
        run_result.adapter_response = {"code": "SELECT"}
        test_runner.print_result_line(run_result)
        assert len(log_test_result_catcher.caught_events) == 1
        name = log_test_result_catcher.caught_events[0].data.name
        assert "(" not in name


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
