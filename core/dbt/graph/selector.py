
from typing import (
    Set, List, Dict, Union, Type
)

from .graph import Graph, UniqueId
from .queue import GraphQueue
from .selector_methods import (
    MethodName,
    SelectorMethod,
    QualifiedNameSelectorMethod,
    TagSelectorMethod,
    SourceSelectorMethod,
    PathSelectorMethod,
)
from .selector_spec import SelectionCriteria, SelectionSpec

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType
from dbt.exceptions import InternalException, warn_or_error
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


class InvalidSelectorError(Exception):
    pass


class NodeSelector:
    """The node selector is aware of the graph and manifest,
    """
    SELECTOR_METHODS: Dict[str, Type[SelectorMethod]] = {}

    def __init__(
        self,
        graph: Graph,
        manifest: Manifest,
    ):
        self.full_graph = graph
        self.manifest = manifest

    @classmethod
    def register_method(cls, name, selector: Type[SelectorMethod]):
        cls.SELECTOR_METHODS[name] = selector

    def get_selector(self, method: str) -> SelectorMethod:
        if method in self.SELECTOR_METHODS:
            cls: Type[SelectorMethod] = self.SELECTOR_METHODS[method]
            return cls(self.manifest)
        else:
            raise InvalidSelectorError(method)

    def select_included(
        self, included_nodes: Set[str], spec: SelectionCriteria,
    ) -> Set[str]:
        selector = self.get_selector(spec.method)
        return set(selector.search(included_nodes, spec.value))

    def get_nodes_from_criteria(
        self, graph: Graph, spec: SelectionCriteria
    ) -> Set[str]:
        nodes = graph.nodes()
        try:
            collected = self.select_included(nodes, spec)
        except InvalidSelectorError:
            valid_selectors = ", ".join(self.SELECTOR_METHODS)
            logger.info(
                f"The '{spec.method}' selector specified in {spec.raw} is "
                f"invalid. Must be one of [{valid_selectors}]"
            )
            return set()

        specified = self.collect_models(spec, graph, collected)
        collected.update(specified)
        result = self.expand_selection(graph, collected)
        return result

    def collect_models(
        self, spec: SelectionCriteria, graph: Graph, selected: Set[UniqueId]
    ) -> Set[UniqueId]:
        additional: Set[UniqueId] = set()
        if spec.select_childrens_parents:
            additional.update(graph.select_childrens_parents(selected))
        if spec.select_parents:
            additional.update(
                graph.select_parents(selected, spec.select_parents_max_depth)
            )
        if spec.select_children:
            additional.update(
                graph.select_children(selected, spec.select_children_max_depth)
            )
        return additional

    def select_nodes(
        self, graph: Graph, spec: SelectionSpec
    ) -> Set[str]:
        if isinstance(spec, SelectionCriteria):
            result = self.get_nodes_from_criteria(graph, spec)
        else:
            node_selections = [
                self.select_nodes(graph, component)
                for component in spec
            ]
            if node_selections:
                result = spec.combine_selections(node_selections)
            else:
                result = set()
            if spec.expect_exists:
                alert_non_existence(spec.raw, result)
        return result

    def _is_graph_member(self, unique_id: str) -> bool:
        if unique_id in self.manifest.sources:
            source = self.manifest.sources[unique_id]
            return source.config.enabled
        node = self.manifest.nodes[unique_id]
        return not node.empty and node.config.enabled

    def node_is_match(
        self,
        node: Union[ParsedSourceDefinition, NonSourceNode],
    ) -> bool:
        return True

    def _is_match(self, unique_id: str) -> bool:
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

    def build_graph_member_subgraph(self) -> Graph:
        graph_members = {
            unique_id for unique_id in self.full_graph.nodes()
            if self._is_graph_member(unique_id)
        }
        return self.full_graph.subgraph(graph_members)

    def filter_selection(self, selected: Set[str]) -> Set[str]:
        return {
            unique_id for unique_id in selected if self._is_match(unique_id)
        }

    def expand_selection(
        self, filtered_graph: Graph, selected: Set[str]
    ) -> Set[str]:
        return selected

    def get_selected(self, spec: SelectionCriteria) -> Set[str]:
        """get_selected runs trhough the node selection process:

            - build a subgraph containing only non-empty, enabled nodes and
                enabled sources.
            - node selection. Based on the include/exclude sets, the set
                of matched unique IDs is returned
                - expand the graph at each leaf node, before combination
                    - selectors might override this. for example, this is where
                        tests are added
            - filtering:
                - selectors can filter the nodes after all of them have been
                  selected
        """
        filtered_graph = self.build_graph_member_subgraph()
        selected_nodes = self.select_nodes(filtered_graph, spec)
        filtered_nodes = self.filter_selection(selected_nodes)
        return filtered_nodes

    def get_graph_queue(self, spec: SelectionCriteria) -> GraphQueue:
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


NodeSelector.register_method(MethodName.FQN, QualifiedNameSelectorMethod)
NodeSelector.register_method(MethodName.Tag, TagSelectorMethod)
NodeSelector.register_method(MethodName.Source, SourceSelectorMethod)
NodeSelector.register_method(MethodName.Path, PathSelectorMethod)
