from dataclasses import dataclass
from typing import AbstractSet, Any, Dict, List, Optional, Tuple

import networkx as nx
import pytest

from dbt.artifacts.resources.types import NodeType
from dbt.graph import Graph, ResourceTypeSelector
from dbt.task.runnable import GraphRunnableMode, GraphRunnableTask
from dbt.tests.util import safe_set_invocation_context
from tests.unit.utils import MockNode, make_manifest


@dataclass
class MockArgs:
    """Simple mock args for us in a runnable task"""

    state: Optional[Dict[str, Any]] = None
    defer_state: Optional[Dict[str, Any]] = None
    write_json: bool = False
    selector: Optional[str] = None
    select: Tuple[str] = ()
    exclude: Tuple[str] = ()


@dataclass
class MockConfig:
    """Simple mock config for use in a RunnableTask"""

    threads: int = 1
    target_name: str = "mock_config_target_name"

    def get_default_selector_name(self):
        return None


class MockRunnableTask(GraphRunnableTask):
    def __init__(
        self,
        exception_class: Exception = Exception,
        nodes: Optional[List[MockNode]] = None,
        edges: Optional[List[Tuple[str, str]]] = None,
    ):
        nodes = nodes or []
        edges = edges or []

        self.forced_exception_class = exception_class
        self.did_cancel: bool = False
        super().__init__(args=MockArgs(), config=MockConfig(), manifest=None)
        self.manifest = make_manifest(nodes=nodes)
        digraph = nx.DiGraph()
        for edge in edges:
            digraph.add_edge(edge[0], edge[1])
        self.graph = Graph(digraph)

    def run_queue(self, pool):
        """Override `run_queue` to raise a system exit"""
        raise self.forced_exception_class()

    def _cancel_connections(self, pool):
        """Override `_cancel_connections` to track whether it was called"""
        self.did_cancel = True

    def get_node_selector(self):
        """This is an `abstract_method` on `GraphRunnableTask`, thus we must implement it"""
        selector = ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[NodeType.Model],
            include_empty_nodes=True,
        )
        return selector

    def defer_to_manifest(self, adapter, selected_uids: AbstractSet[str]):
        """This is an `abstract_method` on `GraphRunnableTask`, thus we must implement it"""
        return None


class MockRunnableTaskIndependent(MockRunnableTask):
    def get_run_mode(self) -> GraphRunnableMode:
        return GraphRunnableMode.Independent


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


def test_graph_runnable_preserves_edges_by_default():
    task = MockRunnableTask(
        nodes=[
            MockNode("test", "upstream_node", fqn="model.test.upstream_node"),
            MockNode("test", "downstream_node", fqn="model.test.downstream_node"),
        ],
        edges=[("model.test.upstream_node", "model.test.downstream_node")],
    )
    assert task.get_run_mode() == GraphRunnableMode.Topological
    graph_queue = task.get_graph_queue()

    assert graph_queue.queued == {"model.test.upstream_node"}
    assert graph_queue.inner.queue == [(0, "model.test.upstream_node")]


def test_graph_runnable_preserves_edges_false():
    task = MockRunnableTaskIndependent(
        nodes=[
            MockNode("test", "upstream_node", fqn="model.test.upstream_node"),
            MockNode("test", "downstream_node", fqn="model.test.downstream_node"),
        ],
        edges=[("model.test.upstream_node", "model.test.downstream_node")],
    )
    assert task.get_run_mode() == GraphRunnableMode.Independent
    graph_queue = task.get_graph_queue()

    assert graph_queue.queued == {"model.test.downstream_node", "model.test.upstream_node"}
    assert graph_queue.inner.queue == [
        (0, "model.test.downstream_node"),
        (0, "model.test.upstream_node"),
    ]
