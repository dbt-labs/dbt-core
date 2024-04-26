import pytest

from dataclasses import dataclass
from dbt.task.runnable import GraphRunnableTask
from typing import AbstractSet, Any, Dict, Optional

from dbt.tests.util import safe_set_invocation_context


@dataclass
class MockArgs:
    """Simple mock args for us in a runnable task"""

    state: Optional[Dict[str, Any]] = None
    defer_state: Optional[Dict[str, Any]] = None
    write_json: bool = False


@dataclass
class MockConfig:
    """Simple mock config for use in a RunnableTask"""

    threads: int = 1
    target_name: str = "mock_config_target_name"


class MockRunnableTask(GraphRunnableTask):
    def __init__(self, exception_class: Exception = Exception):
        self.forced_exception_class = exception_class
        self.did_cancel: bool = False
        super().__init__(args=MockArgs(), config=MockConfig(), manifest=None)

    def run_queue(self, pool):
        """Override `run_queue` to raise a system exit"""
        raise self.forced_exception_class()

    def _cancel_connections(self, pool):
        """Override `_cancel_connections` to track whether it was called"""
        self.did_cancel = True

    def get_node_selector(self):
        """This is an `abstract_method` on `GraphRunnableTask`, thus we must implement it"""
        return None

    def defer_to_manifest(self, adapter, selected_uids: AbstractSet[str]):
        """This is an `abstract_method` on `GraphRunnableTask`, thus we must implement it"""
        return None


def test_graph_runnable_task_cancels_connection_on_system_exit():

    safe_set_invocation_context()

    task = MockRunnableTask(exception_class=SystemExit)

    with pytest.raises(SystemExit):
        task.execute_nodes()

    # If `did_cancel` is True, that means `_cancel_connections` was called
    assert task.did_cancel is True


def test_graph_runnable_task_cancels_connection_on_keyboard_interrupt():

    safe_set_invocation_context()

    task = MockRunnableTask(exception_class=KeyboardInterrupt)

    with pytest.raises(KeyboardInterrupt):
        task.execute_nodes()

    # If `did_cancel` is True, that means `_cancel_connections` was called
    assert task.did_cancel is True


def test_graph_runnable_task_doesnt_cancel_connection_on_generic_exception():
    task = MockRunnableTask(exception_class=Exception)

    with pytest.raises(Exception):
        task.execute_nodes()

    # If `did_cancel` is True, that means `_cancel_connections` was called
    assert task.did_cancel is False
