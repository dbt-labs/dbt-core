from unittest import mock

import networkx as nx
import pytest

from dbt.contracts.graph.manifest import Manifest
from dbt.graph.queue import GraphQueue
from tests.unit.utils import MockNode, make_manifest


class TestGraphQueue:
    @pytest.fixture(scope="class")
    def manifest(self) -> Manifest:
        return make_manifest(
            nodes=[
                MockNode(package="test_package", name="upstream_model"),
                MockNode(package="test_package", name="downstream_model"),
            ]
        )

    @pytest.fixture(scope="class")
    def graph(self) -> nx.DiGraph:
        graph = nx.DiGraph()
        graph.add_edge("model.test_package.upstream_model", "model.test_package.downstream_model")
        return graph

    def test_init_graph_queue(self, manifest, graph):
        graph_queue = GraphQueue(graph=graph, manifest=manifest, selected={})

        assert graph_queue.manifest == manifest
        assert graph_queue.graph == graph
        assert graph_queue.inner.queue == [(0, 0, "model.test_package.upstream_model")]
        assert graph_queue.in_progress == set()
        assert graph_queue.queued == {"model.test_package.upstream_model"}
        assert graph_queue.lock

    def test_init_graph_queue_preserve_edges_false(self, manifest, graph):
        graph_queue = GraphQueue(graph=graph, manifest=manifest, selected={}, preserve_edges=False)

        # when preserve_edges is set to false, dependencies between nodes are no longer tracked in the priority queue
        assert list(graph_queue.graph.edges) == []
        assert graph_queue.inner.queue == [
            (0, 0, "model.test_package.downstream_model"),
            (0, 0, "model.test_package.upstream_model"),
        ]
        assert graph_queue.queued == {
            "model.test_package.upstream_model",
            "model.test_package.downstream_model",
        }

    def test_priority_tie_breaker(self):
        # If config.priority is missing or invalid, it falls back to 0.
        default_priority = MockNode(
            package="test_package",
            name="aaa_default_priority",
            config=mock.MagicMock(),
        )
        low_priority = MockNode(
            package="test_package",
            name="nnn_low_priority",
            config=mock.MagicMock(priority=-10),
        )
        high_priority = MockNode(
            package="test_package",
            name="zzz_high_priority",
            config=mock.MagicMock(priority=10),
        )
        manifest = make_manifest(nodes=[default_priority, low_priority, high_priority])
        graph = nx.DiGraph()
        graph.add_nodes_from(
            [default_priority.unique_id, low_priority.unique_id, high_priority.unique_id]
        )

        graph_queue = GraphQueue(graph=graph, manifest=manifest, selected={})
        first = graph_queue.get(block=False)

        assert first.unique_id == high_priority.unique_id

        graph_queue.mark_done(first.unique_id)
        second = graph_queue.get(block=False)

        assert second.unique_id == default_priority.unique_id

        graph_queue.mark_done(second.unique_id)
        third = graph_queue.get(block=False)

        assert third.unique_id == low_priority.unique_id
