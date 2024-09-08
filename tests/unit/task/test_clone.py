from unittest.mock import MagicMock, patch

from dbt.flags import get_flags
from dbt.task.clone import CloneTask


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
