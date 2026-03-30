import pytest
from unittest.mock import MagicMock, patch

from dbt.artifacts.schemas.results import NodeStatus, RunStatus
from dbt.artifacts.schemas.run import RunResult
from dbt.events.types import LogModelResult, LogStartLine
from dbt.flags import get_flags
from dbt.task.clone import CloneRunner, CloneTask
from dbt_common.events.base_types import EventLevel
from dbt_common.events.event_catcher import EventCatcher
from dbt_common.events.event_manager_client import add_callback_to_manager


def test_clone_task_not_preserve_edges():
    mock_node_selector = MagicMock()
    mock_spec = MagicMock()
    with patch.object(
        CloneTask, "get_node_selector", return_value=mock_node_selector
    ), patch.object(CloneTask, "get_selection_spec", return_value=mock_spec):
        task = CloneTask(get_flags(), None, None)
        task.get_graph_queue()
        # when we get the graph queue, preserve_edges is False
        mock_node_selector.get_graph_queue.assert_called_with(mock_spec, False)


@pytest.fixture
def mock_node():
    node = MagicMock()
    node.unique_id = "model.my_pkg.my_model"
    node.node_info = MagicMock()
    return node


@pytest.fixture
def clone_runner(mock_node):
    return CloneRunner(
        config=MagicMock(),
        adapter=MagicMock(),
        node=mock_node,
        node_index=1,
        num_nodes=3,
    )


class TestCloneRunnerLogging:
    """Verify that CloneRunner emits per-node start and result log lines.
    See https://github.com/dbt-labs/dbt-core/issues/9501
    """

    @pytest.fixture
    def log_start_catcher(self):
        catcher = EventCatcher(event_to_catch=LogStartLine)
        add_callback_to_manager(catcher.catch)
        return catcher

    @pytest.fixture
    def log_result_catcher(self):
        catcher = EventCatcher(event_to_catch=LogModelResult)
        add_callback_to_manager(catcher.catch)
        return catcher

    def _make_result(self, node, status=RunStatus.Success, message="No-op"):
        return RunResult(
            node=node,
            status=status,
            timing=[],
            thread_id="thread-1",
            execution_time=0.5,
            message=message,
            adapter_response={},
            failures=None,
            batch_results=None,
        )

    def test_before_execute_fires_log_start_line(
        self, log_start_catcher, clone_runner
    ):
        clone_runner.before_execute()
        assert len(log_start_catcher.caught_events) == 1
        event = log_start_catcher.caught_events[0]
        assert "clone" in event.data.description

    def test_after_execute_noop_fires_info_result(
        self, log_result_catcher, clone_runner, mock_node
    ):
        result = self._make_result(mock_node, message="No-op")
        clone_runner.after_execute(result)
        assert len(log_result_catcher.caught_events) == 1
        event = log_result_catcher.caught_events[0]
        assert event.info.level == EventLevel.INFO
        assert event.data.status == "No-op"

    def test_after_execute_cloned_fires_info_result(
        self, log_result_catcher, clone_runner, mock_node
    ):
        result = self._make_result(mock_node, message="CREATE TABLE")
        clone_runner.after_execute(result)
        assert len(log_result_catcher.caught_events) == 1
        event = log_result_catcher.caught_events[0]
        assert event.info.level == EventLevel.INFO
        assert event.data.status == "CREATE TABLE"

    def test_after_execute_error_fires_error_result(
        self, log_result_catcher, clone_runner, mock_node
    ):
        result = self._make_result(mock_node, status=RunStatus.Error, message="something failed")
        clone_runner.after_execute(result)
        assert len(log_result_catcher.caught_events) == 1
        event = log_result_catcher.caught_events[0]
        assert event.info.level == EventLevel.ERROR
