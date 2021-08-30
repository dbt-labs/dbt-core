from typing import Set, List, Optional, Tuple

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
from dbt.contracts.graph.compiled import GraphMemberNode
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.state import PreviousState


def get_package_names(nodes):
    return set([node.split(".")[1] for node in nodes])


def alert_non_existence(raw_spec, nodes):
    if len(nodes) == 0:
        warn_or_error(
            f"The selection criterion '{str(raw_spec)}' does not match"
            f" any nodes"
        )


def alert_unused_nodes(raw_spec, node_names):
    summary_nodes_str = ("\n  - ").join(node_names[:3])
    debug_nodes_str = ("\n  - ").join(node_names)
    and_more_str = f"\n  - and {len(node_names) - 3} more" if len(node_names) > 4 else ""
    summary_msg = (
        f"\nSome tests were excluded because at least one parent is not selected. "
        f"Use the --greedy flag to include them."
        f"\n  - {summary_nodes_str}{and_more_str}"
    )
    logger.info(summary_msg)
    if len(node_names) > 4:
        debug_msg = (
            f"Full list of tests that were excluded:"
            f"\n  - {debug_nodes_str}"
        )
        logger.debug(debug_msg)


def can_select_indirectly(node):
    """If a node is not selected itself, but its parent(s) are, it may qualify
    for indirect selection.
    Today, only Test nodes can be indirectly selected. In the future,
    other node types or invocation flags might qualify.
    """
    if node.resource_type == NodeType.Test:
        return True
    else:
        return False


class NodeSelector(MethodManager):
    """The node selector is aware of the graph and manifest,
    """
    def __init__(
        self,
        graph: Graph,
        manifest: Manifest,
        previous_state: Optional[PreviousState] = None,
    ):
        super().__init__(manifest, previous_state)
        self.full_graph = graph

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
        spec: SelectionCriteria
    ) -> Tuple[Set[UniqueId], Set[UniqueId]]:
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
            return set(), set()

        neighbors = self.collect_specified_neighbors(spec, collected)
        direct_nodes, indirect_nodes = self.expand_selection(
            selected=(collected | neighbors),
            greedy=spec.greedy
        )
        return direct_nodes, indirect_nodes

    def collect_specified_neighbors(
        self, spec: SelectionCriteria, selected: Set[UniqueId]
    ) -> Set[UniqueId]:
        """Given the set of models selected by the explicit part of the
        selector (like "tag:foo"), apply the modifiers on the spec ("+"/"@").
        Return the set of additional nodes that should be collected (which may
        overlap with the selected set).
        """
        additional: Set[UniqueId] = set()
        if spec.childrens_parents:
            additional.update(self.graph.select_childrens_parents(selected))

        if spec.parents:
            depth = spec.parents_depth
            additional.update(self.graph.select_parents(selected, depth))

        if spec.children:
            depth = spec.children_depth
            additional.update(self.graph.select_children(selected, depth))
        return additional

    def select_nodes_recursively(self, spec: SelectionSpec) -> Tuple[Set[UniqueId], Set[UniqueId]]:
        """If the spec is a composite spec (a union, difference, or intersection),
        recurse into its selections and combine them. If the spec is a concrete
        selection criteria, resolve that using the given graph.
        """
        if isinstance(spec, SelectionCriteria):
            direct_nodes, indirect_nodes = self.get_nodes_from_criteria(spec)
        else:
            bundles = [
                self.select_nodes_recursively(component)
                for component in spec
            ]

            direct_sets = []
            indirect_sets = []

            for direct, indirect in bundles:
                direct_sets.append(direct)
                indirect_sets.append(direct | indirect)

            initial_direct = spec.combined(direct_sets)
            indirect_nodes = spec.combined(indirect_sets)

            direct_nodes = self.incorporate_indirect_nodes(initial_direct, indirect_nodes)

            if spec.expect_exists:
                alert_non_existence(spec.raw, direct_nodes)

        return direct_nodes, indirect_nodes

    def select_nodes(self, spec: SelectionSpec) -> Tuple[Set[UniqueId], Set[UniqueId]]:
        """Select the nodes in the graph according to the spec.

        This is the main point of entry for turning a spec into a set of nodes:
        - Recurse through spec, select by criteria, combine by set operation
        - Return final (unfiltered) selection set
        """
        direct_nodes, indirect_nodes = self.select_nodes_recursively(spec)
        indirect_only = indirect_nodes.difference(direct_nodes)
        return direct_nodes, indirect_only

    def _is_graph_member(self, unique_id: UniqueId) -> bool:
        if unique_id in self.manifest.sources:
            source = self.manifest.sources[unique_id]
            return source.config.enabled
        elif unique_id in self.manifest.exposures:
            return True
        node = self.manifest.nodes[unique_id]
        return not node.empty and node.config.enabled

    def node_is_match(self, node: GraphMemberNode) -> bool:
        """Determine if a node is a match for the selector. Non-match nodes
        will be excluded from results during filtering.
        """
        return True

    def _is_match(self, unique_id: UniqueId) -> bool:
        node: GraphMemberNode
        if unique_id in self.manifest.nodes:
            node = self.manifest.nodes[unique_id]
        elif unique_id in self.manifest.sources:
            node = self.manifest.sources[unique_id]
        elif unique_id in self.manifest.exposures:
            node = self.manifest.exposures[unique_id]
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

    def expand_selection(
        self, selected: Set[UniqueId], greedy: bool = False
    ) -> Tuple[Set[UniqueId], Set[UniqueId]]:
        # Test selection can expand to include an implicitly/indirectly selected test.
        # In this way, `dbt test -m model_a` also includes tests that directly depend on `model_a`.
        # Expansion has two modes, GREEDY and NOT GREEDY.
        #
        # GREEDY mode: If ANY parent is selected, select the test. We use this for EXCLUSION.
        #
        # NOT GREEDY mode:
        #  - If ALL parents are selected, select the test.
        #  - If ANY parent is missing, return it separately. We'll keep it around
        #    for later and see if its other parents show up.
        # We use this for INCLUSION.
        # Users can also opt in to inclusive GREEDY mode by passing --greedy flag,
        # or by specifying `greedy: true` in a yaml selector

        direct_nodes = set(selected)
        indirect_nodes = set()

        for unique_id in self.graph.select_successors(selected):
            if unique_id in self.manifest.nodes:
                node = self.manifest.nodes[unique_id]
                if can_select_indirectly(node):
                    # should we add it in directly?
                    if greedy or set(node.depends_on.nodes) <= set(selected):
                        direct_nodes.add(unique_id)
                    # if not:
                    else:
                        indirect_nodes.add(unique_id)

        return direct_nodes, indirect_nodes

    def incorporate_indirect_nodes(
        self, direct_nodes: Set[UniqueId], indirect_nodes: Set[UniqueId] = set()
    ) -> Set[UniqueId]:
        # Check tests previously selected indirectly to see if ALL their
        # parents are now present.

        selected = set(direct_nodes)

        for unique_id in indirect_nodes:
            if unique_id in self.manifest.nodes:
                node = self.manifest.nodes[unique_id]
                if set(node.depends_on.nodes) <= set(selected):
                    selected.add(unique_id)

        return selected

    def get_selected(self, spec: SelectionSpec) -> Set[UniqueId]:
        """get_selected runs through the node selection process:

            - node selection. Based on the include/exclude sets, the set
                of matched unique IDs is returned
                - includes direct + indirect selection (for tests)
            - filtering:
                - selectors can filter the nodes after all of them have been
                  selected
        """
        selected_nodes, indirect_only = self.select_nodes(spec)
        filtered_nodes = self.filter_selection(selected_nodes)

        if indirect_only:
            filtered_unused_nodes = self.filter_selection(indirect_only)
            if filtered_unused_nodes and spec.greedy_warning:
                # log anything that didn't make the cut
                unused_node_names = []
                for unique_id in filtered_unused_nodes:
                    name = self.manifest.nodes[unique_id].name
                    unused_node_names.append(name)
                alert_unused_nodes(spec, unused_node_names)

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
        previous_state: Optional[PreviousState],
        resource_types: List[NodeType],
    ):
        super().__init__(
            graph=graph,
            manifest=manifest,
            previous_state=previous_state,
        )
        self.resource_types: Set[NodeType] = set(resource_types)

    def node_is_match(self, node):
        return node.resource_type in self.resource_types
