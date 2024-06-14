import pytest

from dbt.compilation import Linker
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode
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

    def test_descendantcs(self, graph: Graph, manifest: Manifest) -> None:
        model: ModelNode = manifest.nodes["model.pkg.ephemeral_model"]

        # check result when not limiting the depth
        descendants = graph.descendants(node=model.unique_id)
        assert descendants == {
            "test.pkg.view_test_nothing",
            "model.pkg.view_model",
            "model.pkg.table_model",
        }

        # check that result excludes nodes that are out of depth
        descendants = graph.descendants(node=model.unique_id, max_depth=1)
        assert descendants == {"model.pkg.table_model", "model.pkg.view_model"}

    def test_ancestors(self, graph: Graph, manifest: Manifest) -> None:
        model: ModelNode = manifest.nodes["model.pkg.table_model"]

        # check result when not limiting the depth
        ancestors = graph.ancestors(node=model.unique_id)

        assert ancestors == {
            "model.pkg.ephemeral_model",
            "source.pkg.raw.seed",
        }

        # check that result excludes nodes that are out of depth
        ancestors = graph.ancestors(node=model.unique_id, max_depth=1)
        assert ancestors == {"model.pkg.ephemeral_model"}
