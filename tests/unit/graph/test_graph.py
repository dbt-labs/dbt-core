import pytest

from dbt.compilation import Linker
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode
from dbt.graph.graph import Graph
from tests.unit.utils.manifest import make_model


class TestGraph:
    @pytest.fixture
    def extra_parent_model(self) -> ModelNode:
        return make_model(pkg="pkg", name="extra_parent_model", code="SELECT 'cats' as interests")

    @pytest.fixture
    def non_shared_child_of_extra(self, extra_parent_model: ModelNode) -> ModelNode:
        return make_model(
            pkg="pkg",
            name="non_shared_child_of_extra",
            code='SELECT * FROM {{ ref("extra_parent_model") }}',
            refs=[extra_parent_model],
        )

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

    @pytest.fixture(autouse=True)
    def local_manifest_extensions(
        self,
        manifest: Manifest,
        model_with_two_direct_parents: ModelNode,
        non_shared_child_of_extra: ModelNode,
        extra_parent_model: ModelNode,
    ) -> Manifest:
        manifest.add_node_nofile(extra_parent_model)
        manifest.add_node_nofile(non_shared_child_of_extra)
        manifest.add_node_nofile(model_with_two_direct_parents)

    @pytest.fixture
    def graph(self, manifest: Manifest, local_manifest_extensions) -> Graph:
        # We include the `local_manifest_extensions` in the arguments to ensure
        # that fixture adds our extra node sbefore creating the graph
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
            "model.pkg.model_with_two_direct_parents",
            "test.pkg.view_test_nothing",
            "model.pkg.view_model",
            "model.pkg.table_model",
        }

        # check that result excludes nodes that are out of depth
        descendants = graph.descendants(node=model.unique_id, max_depth=1)
        assert descendants == {
            "model.pkg.model_with_two_direct_parents",
            "model.pkg.table_model",
            "model.pkg.view_model",
        }

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
        graph: Graph,
        model_with_two_direct_parents: ModelNode,
        extra_parent_model: ModelNode,
        ephemeral_model: ModelNode,
    ) -> None:
        # `select_childrens_parents` should return
        # * all children of the selected node (without depth limit)
        # * all parents of the children of the selected node (without depth limit)
        childrens_parents = graph.select_childrens_parents(
            selected={
                extra_parent_model.unique_id,
            }
        )

        assert model_with_two_direct_parents.unique_id in childrens_parents
        assert extra_parent_model.unique_id in childrens_parents
        assert ephemeral_model.unique_id in childrens_parents
        assert len(childrens_parents) == 5

    def test_select_children(
        self,
        graph: Graph,
        ephemeral_model: ModelNode,
        extra_parent_model: ModelNode,
    ) -> None:
        ephemerals_children = graph.select_children(selected={ephemeral_model.unique_id})
        extras_children = graph.select_children(selected={extra_parent_model.unique_id})
        joint_children = graph.select_children(
            selected={extra_parent_model.unique_id, ephemeral_model.unique_id}
        )

        assert joint_children == ephemerals_children.union(extras_children)
        # These additional assertions are because we intentionally setup the test such that
        # neither nodes children set is a subset of the other
        assert not ephemerals_children.issubset(extras_children)
        assert not extras_children.issubset(ephemerals_children)

    def test_select_parents(
        self,
        graph: Graph,
        non_shared_child_of_extra: ModelNode,
        table_model: ModelNode,
    ) -> None:
        non_shareds_parents = graph.select_parents(selected={non_shared_child_of_extra.unique_id})
        tables_parents = graph.select_parents(selected={table_model.unique_id})
        joint_parents = graph.select_parents(
            selected={table_model.unique_id, non_shared_child_of_extra.unique_id}
        )

        assert joint_parents == tables_parents.union(non_shareds_parents)
        # These additional assertions are because we intentionally setup the test such that
        # neither nodes parents set is a subset of the other
        assert not non_shareds_parents.issubset(tables_parents)
        assert not tables_parents.issubset(non_shareds_parents)
