import pytest

from dbt.compilation import Linker
from dbt.contracts.graph.manifest import Manifest
from dbt.graph.graph import Graph


class TestGraph:
    @pytest.fixture
    def graph(self, manifest: Manifest) -> Graph:
        linker = Linker()
        linker.link_graph(manifest=manifest)
        return Graph(graph=linker.graph)

    def test_nodes(self, graph: Graph, manifest: Manifest):
        graph_nodes = graph.nodes()
        all_manifest_nodes = []
        for resources in manifest.get_resource_fqns().values():
            all_manifest_nodes.extend(list(resources))

        # Assert that it is a set, thus no duplicates
        assert isinstance(graph_nodes, set)
        assert len(graph_nodes) == len(all_manifest_nodes)
