from argparse import Namespace
from unittest.mock import MagicMock, patch

import pytest

from dbt.adapters.postgres import PostgresAdapter
from dbt.artifacts.schemas.results import RunStatus
from dbt.artifacts.schemas.run import RunResult
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode
from dbt.events.types import LogModelResult
from dbt.flags import get_flags, set_from_args
from dbt.task.run import ModelRunner, RunTask
from dbt.tests.util import safe_set_invocation_context
from dbt_common.events.base_types import EventLevel
from dbt_common.events.event_manager_client import add_callback_to_manager
from tests.utils import EventCatcher


@pytest.mark.parametrize(
    "exception_to_raise, expected_cancel_connections",
    [
        (SystemExit, True),
        (KeyboardInterrupt, True),
        (Exception, False),
    ],
)
def test_run_task_cancel_connections(
    exception_to_raise, expected_cancel_connections, runtime_config: RuntimeConfig
):
    safe_set_invocation_context()

    def mock_run_queue(*args, **kwargs):
        raise exception_to_raise("Test exception")

    with patch.object(RunTask, "run_queue", mock_run_queue), patch.object(
        RunTask, "_cancel_connections"
    ) as mock_cancel_connections:

        set_from_args(Namespace(write_json=False), None)
        task = RunTask(
            get_flags(),
            runtime_config,
            None,
        )
        with pytest.raises(exception_to_raise):
            task.execute_nodes()
        assert mock_cancel_connections.called == expected_cancel_connections


def test_run_task_preserve_edges():
    mock_node_selector = MagicMock()
    mock_spec = MagicMock()
    with patch.object(RunTask, "get_node_selector", return_value=mock_node_selector), patch.object(
        RunTask, "get_selection_spec", return_value=mock_spec
    ):
        task = RunTask(get_flags(), None, None)
        task.get_graph_queue()
        # when we get the graph queue, preserve_edges is True
        mock_node_selector.get_graph_queue.assert_called_with(mock_spec, True)


class TestModelRunner:
    @pytest.fixture
    def log_model_result_catcher(self) -> EventCatcher:
        catcher = EventCatcher(event_to_catch=LogModelResult)
        add_callback_to_manager(catcher.catch)
        return catcher

    @pytest.fixture
    def model_runner(
        self,
        postgres_adapter: PostgresAdapter,
        table_model: ModelNode,
        runtime_config: RuntimeConfig,
    ) -> ModelRunner:
        return ModelRunner(
            config=runtime_config,
            adapter=postgres_adapter,
            node=table_model,
            node_index=1,
            num_nodes=1,
        )

    @pytest.fixture
    def run_result(self, table_model: ModelNode) -> RunResult:
        return RunResult(
            status=RunStatus.Success,
            timing=[],
            thread_id="an_id",
            execution_time=0,
            adapter_response={},
            message="It did it",
            failures=None,
            node=table_model,
        )

    def test_print_result_line(
        self,
        log_model_result_catcher: EventCatcher,
        model_runner: ModelRunner,
        run_result: RunResult,
    ) -> None:
        # Check `print_result_line` with "successful" RunResult
        model_runner.print_result_line(run_result)
        assert len(log_model_result_catcher.caught_events) == 1
        assert log_model_result_catcher.caught_events[0].info.level == EventLevel.INFO
        assert log_model_result_catcher.caught_events[0].data.status == run_result.message

        # reset event catcher
        log_model_result_catcher.flush()

        # Check `print_result_line` with "error" RunResult
        run_result.status = RunStatus.Error
        model_runner.print_result_line(run_result)
        assert len(log_model_result_catcher.caught_events) == 1
        assert log_model_result_catcher.caught_events[0].info.level == EventLevel.ERROR
        assert log_model_result_catcher.caught_events[0].data.status == EventLevel.ERROR

    @pytest.mark.skip(
        reason="Default and adapter macros aren't being appropriately populated, leading to a runtime error"
    )
    def test_execute(
        self, table_model: ModelNode, manifest: Manifest, model_runner: ModelRunner
    ) -> None:
        model_runner.execute(model=table_model, manifest=manifest)
        # TODO: Assert that the model was executed
