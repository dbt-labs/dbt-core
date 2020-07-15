
from typing import Set, List, Union

from .graph import Graph, UniqueId
from .queue import GraphQueue
from .selector_methods import MethodManager
from .selector_spec import SelectionCriteria, SelectionSpec

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType
from dbt.exceptions import (
    InternalException,
    InvalidSelectorException,
    warn_or_error,
)
from dbt.contracts.graph.compiled import NonSourceNode, CompileResultNode
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import ParsedSourceDefinition


def get_package_names(nodes):
    return set([node.split(".")[1] for node in nodes])


def alert_non_existence(raw_spec, nodes):
    if len(nodes) == 0:
        warn_or_error(
            f"The selector '{str(raw_spec)}' does not match any nodes and will"
            f" be ignored"
        )


class NodeSelector(MethodManager):
    """The node selector is aware of the graph and manifest,
    """
    def __init__(
        self,
        graph: Graph,
        manifest: Manifest,
    ):
        self.full_graph = graph
        self.manifest = manifest

        # build a subgraph containing only non-empty, enabled nodes and enabled
        # sources.
        graph_members = {
            unique_id for unique_id in self.full_graph.nodes()
            if self._is_graph_member(unique_id)
        }
        self.graph = self.full_graph.subgraph(graph_members)

    def select_included(
        self, included_nodes: Set[UniqueId], spec: SelectionCriteria,
    ) -> Set[UniqueId]:
        """Select the explicitly included nodes, using the given spec. Return
        the selected set of unique IDs.
        """
        method = self.get_method(spec.method, spec.method_arguments)
        return set(method.search(included_nodes, spec.value))

    def get_nodes_from_criteria(
        self,
        spec: SelectionCriteria,
    ) -> Set[UniqueId]:
        """Get all nodes specified by the single selection criteria.

        - collect the directly included nodes
        - find their specified relatives
        - perform any selector-specific expansion
        """

        nodes = self.graph.nodes()
        try:
            collected = self.select_included(nodes, spec)
        except InvalidSelectorException:
            valid_selectors = ", ".join(self.SELECTOR_METHODS)
            logger.info(
                f"The '{spec.method}' selector specified in {spec.raw} is "
                f"invalid. Must be one of [{valid_selectors}]"
            )
            return set()

        extras = self.collect_specified_neighbors(spec, collected)
        result = self.expand_selection(collected | extras)
        return result

    def collect_specified_neighbors(
        self, spec: SelectionCriteria, selected: Set[UniqueId]
    ) -> Set[UniqueId]:
        """Given the set of models selected by the explicit part of the
        selector (like "tag:foo"), apply the modifiers on the spec ("+"/"@").
        Return the set of additional nodes that should be collected (which may
        overlap with the selected set).
        """
        additional: Set[UniqueId] = set()
        if spec.select_childrens_parents:
            additional.update(self.graph.select_childrens_parents(selected))

        if spec.select_parents:
            depth = spec.select_parents_max_depth
            additional.update(self.graph.select_parents(selected, depth))

        if spec.select_children:
            depth = spec.select_children_max_depth
            additional.update(self.graph.select_children(selected, depth))
        return additional

    def select_nodes(self, spec: SelectionSpec) -> Set[UniqueId]:
        """Select the nodes in the graph according to the spec.

        If the spec is a composite spec (a union, difference, or intersection),
        recurse into its selections and combine them. If the spec is a concrete
        selection criteria, resolve that using the given graph.
        """
        if isinstance(spec, SelectionCriteria):
            result = self.get_nodes_from_criteria(spec)
        else:
            node_selections = [
                self.select_nodes(component)
                for component in spec
            ]
            result = spec.combined(node_selections)
            if spec.expect_exists:
                alert_non_existence(spec.raw, result)
        return result

    def _is_graph_member(self, unique_id: UniqueId) -> bool:
        if unique_id in self.manifest.sources:
            source = self.manifest.sources[unique_id]
            return source.config.enabled
        node = self.manifest.nodes[unique_id]
        return not node.empty and node.config.enabled

    def node_is_match(
        self,
        node: Union[ParsedSourceDefinition, NonSourceNode],
    ) -> bool:
        """Determine if a node is a match for the selector. Non-match nodes
        will be excluded from results during filtering.
        """
        return True

    def _is_match(self, unique_id: UniqueId) -> bool:
        node: CompileResultNode
        if unique_id in self.manifest.nodes:
            node = self.manifest.nodes[unique_id]
        elif unique_id in self.manifest.sources:
            node = self.manifest.sources[unique_id]
        else:
            raise InternalException(
                f'Node {unique_id} not found in the manifest!'
            )
        return self.node_is_match(node)

    def filter_selection(self, selected: Set[UniqueId]) -> Set[UniqueId]:
        """Return the subset of selected nodes that is a match for this
        selector.
        """
        return {
            unique_id for unique_id in selected if self._is_match(unique_id)
        }

    def expand_selection(self, selected: Set[UniqueId]) -> Set[UniqueId]:
        """Perform selector-specific expansion."""
        return selected

    def get_selected(self, spec: SelectionSpec) -> Set[UniqueId]:
        """get_selected runs trhough the node selection process:

            - node selection. Based on the include/exclude sets, the set
                of matched unique IDs is returned
                - expand the graph at each leaf node, before combination
                    - selectors might override this. for example, this is where
                        tests are added
            - filtering:
                - selectors can filter the nodes after all of them have been
                  selected
        """
        selected_nodes = self.select_nodes(spec)
        filtered_nodes = self.filter_selection(selected_nodes)
        return filtered_nodes

    def get_graph_queue(self, spec: SelectionSpec) -> GraphQueue:
        """Returns a queue over nodes in the graph that tracks progress of
        dependecies.
        """
        selected_nodes = self.get_selected(spec)
        new_graph = self.full_graph.get_subset_graph(selected_nodes)
        # should we give a way here for consumers to mutate the graph?
        return GraphQueue(new_graph.graph, self.manifest, selected_nodes)


class ResourceTypeSelector(NodeSelector):
    def __init__(
        self,
        graph: Graph,
        manifest: Manifest,
        resource_types: List[NodeType],
    ):
        super().__init__(
            graph=graph,
            manifest=manifest,
        )
        self.resource_types: Set[NodeType] = set(resource_types)

    def node_is_match(self, node):
        return node.resource_type in self.resource_types
