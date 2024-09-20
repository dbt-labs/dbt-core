from argparse import Namespace
from unittest.mock import patch

from dbt.flags import get_flags, set_from_args
from dbt.task.list import ListTask
from dbt_common.events.types import PrintEvent


def test_list_output_results():
    set_from_args(Namespace(models=None), {})
    task = ListTask(get_flags(), None, None)
    results = ["node1", "node2", "node3"]
    expected_node_results = ["node1", "node2", "node3"]

    with patch("dbt.task.list.fire_event") as mock_fire_event:
        node_results = task.output_results(results)

    assert node_results == expected_node_results
    # assert called with PrintEvent type object and message 'node1', 'node2', 'node3'
    for call_args in mock_fire_event.call_args_list:
        assert isinstance(call_args[0][0], PrintEvent)
        assert call_args[0][0].msg in expected_node_results
