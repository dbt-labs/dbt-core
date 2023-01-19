from typing import Set, Iterable, Iterator, Optional, NewType
from itertools import product
import networkx as nx  # type: ignore

from dbt.exceptions import DbtInternalError

UniqueId = NewType("UniqueId", str)


class Graph:
    """A wrapper around the networkx graph that understands SelectionCriteria
    and how they interact with the graph.
    """

    def __init__(self, graph):
        self.graph = graph

    def nodes(self) -> Set[UniqueId]:
        return set(self.graph.nodes())

    def edges(self):
        return self.graph.edges()

    def __iter__(self) -> Iterator[UniqueId]:
        return iter(self.graph.nodes())

    def ancestors(self, node: UniqueId, max_depth: Optional[int] = None) -> Set[UniqueId]:
        """Returns all nodes having a path to `node` in `graph`"""
        if not self.graph.has_node(node):
            raise DbtInternalError(f"Node {node} not found in the graph!")
        return {
            child
            for _, child in nx.bfs_edges(self.graph, node, reverse=True, depth_limit=max_depth)
        }

    def descendants(self, node: UniqueId, max_depth: Optional[int] = None) -> Set[UniqueId]:
        """Returns all nodes reachable from `node` in `graph`"""
        if not self.graph.has_node(node):
            raise DbtInternalError(f"Node {node} not found in the graph!")
        return {child for _, child in nx.bfs_edges(self.graph, node, depth_limit=max_depth)}

    def select_childrens_parents(self, selected: Set[UniqueId]) -> Set[UniqueId]:
        ancestors_for = self.select_children(selected) | selected
        return self.select_parents(ancestors_for) | ancestors_for

    def select_children(
        self, selected: Set[UniqueId], max_depth: Optional[int] = None
    ) -> Set[UniqueId]:
        descendants: Set[UniqueId] = set()
        for node in selected:
            descendants.update(self.descendants(node, max_depth))
        return descendants

    def select_parents(
        self, selected: Set[UniqueId], max_depth: Optional[int] = None
    ) -> Set[UniqueId]:
        ancestors: Set[UniqueId] = set()
        for node in selected:
            ancestors.update(self.ancestors(node, max_depth))
        return ancestors

    def select_successors(self, selected: Set[UniqueId]) -> Set[UniqueId]:
        successors: Set[UniqueId] = set()
        for node in selected:
            successors.update(self.graph.successors(node))
        return successors

    def get_subset_graph(self, selected: Iterable[UniqueId]) -> "Graph":
        """Create and return a new graph that is a shallow copy of the graph,
        but with only the nodes in include_nodes. Transitive edges across
        removed nodes are preserved as explicit new edges.
        """

        new_graph = self.graph.copy()
        include_nodes = set(selected)

        for node in self:
            if node not in include_nodes:
                source_nodes = [x for x, _ in new_graph.in_edges(node)]
                target_nodes = [x for _, x in new_graph.out_edges(node)]

                new_edges = product(source_nodes, target_nodes)
                non_cyclic_new_edges = [
                    (source, target) for source, target in new_edges if source != target
                ]  # removes cyclic refs

                new_graph.add_edges_from(non_cyclic_new_edges)
                new_graph.remove_node(node)

        for node in include_nodes:
            if node not in new_graph:
                raise ValueError(
                    "Couldn't find model '{}' -- does it exist or is it disabled?".format(node)
                )

        return Graph(new_graph)

    def subgraph(self, nodes: Iterable[UniqueId]) -> "Graph":
        return Graph(self.graph.subgraph(nodes))

    def get_dependent_nodes(self, node: UniqueId):
        return nx.descendants(self.graph, node)
