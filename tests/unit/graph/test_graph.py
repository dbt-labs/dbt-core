import pytest

from dbt.compilation import Linker
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode
from dbt.graph.graph import Graph
from tests.unit.utils.manifest import make_model


class TestGraph:
    @pytest.fixture
    def graph(self, manifest: Manifest) -> Graph:
        linker = Linker()
        linker.link_graph(manifest=manifest)
        return Graph(graph=linker.graph)

    @pytest.fixture
    def extra_parent_model(self) -> ModelNode:
        return make_model(pkg="pkg", name="extra_parent_model", code="SELECT 'cats' as interests")

    @pytest.fixture
    def model_with_two_direct_parents(
        self, extra_parent_model: ModelNode, ephemeral_model: ModelNode
    ) -> ModelNode:
        return make_model(
            pkg="pkg",
            name="model_with_two_direct_parents",
            code='SELECT * FROM {{ ref("ephemeral_model") }} UNION ALL SELECT * FROM {{ ref("extra_parent_model") }}',
            refs=[extra_parent_model, ephemeral_model],
        )

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

    @pytest.mark.skip(reason="I haven't figured out how to add edge types to nodes")
    def test_exclude_edge_type(self) -> None:
        # I though something like the following would produce
        # linker = Linker()
        # linker.link_graph(manifest=manifest)
        # linker.add_test_edges(manifest=manifest)
        # graph = Graph(graph=linker.graph)
        pass

    def test_select_childrens_parents(
        self,
        model_with_two_direct_parents: ModelNode,
        extra_parent_model: ModelNode,
        manifest: Manifest,
        ephemeral_model: ModelNode,
    ) -> None:
        # add extra nodes to manifest
        manifest.add_node_nofile(extra_parent_model)
        manifest.add_node_nofile(model_with_two_direct_parents)

        # create graph (we don't use the fixture because we want our extra nodes)
        linker = Linker()
        linker.link_graph(manifest=manifest)
        graph = Graph(graph=linker.graph)

        # the "selected" node we care about
        model: ModelNode = manifest.nodes["model.pkg.extra_parent_model"]

        # `select_childrens_parents` should return
        # * all children of the selected node (without depth limit)
        # * all parents of the children of the selected node (without depth limit)
        childrens_parents = graph.select_childrens_parents(
            selected={
                model.unique_id,
            }
        )

        assert model_with_two_direct_parents.unique_id in childrens_parents
        assert extra_parent_model.unique_id in childrens_parents
        assert ephemeral_model.unique_id in childrens_parents
        assert len(childrens_parents) == 4
