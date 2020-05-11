import os
from enum import Enum
from itertools import chain
from pathlib import Path
from typing import Set, Iterable, Union, List, Container, Tuple, Optional

import networkx as nx  # type: ignore

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import coalesce
from dbt.node_types import NodeType
from dbt.exceptions import RuntimeException, InternalException, warn_or_error

SELECTOR_PARENTS = '+'
SELECTOR_CHILDREN = '+'
SELECTOR_GLOB = '*'
SELECTOR_CHILDREN_AND_ANCESTORS = '@'
SELECTOR_DELIMITER = ':'
SPEC_DELIMITER = ' '
INTERSECTION_DELIMITER = ','


def _probably_path(value: str):
    """Decide if value is probably a path. Windows has two path separators, so
    we should check both sep ('\\') and altsep ('/') there.
    """
    if os.path.sep in value:
        return True
    elif os.path.altsep is not None and os.path.altsep in value:
        return True
    else:
        return False


class SelectionCriteria:
    def __init__(self, node_spec: str):
        self.raw = node_spec
        self.select_children = False
        self.select_parents = False
        self.select_childrens_parents = False

        if node_spec.startswith(SELECTOR_CHILDREN_AND_ANCESTORS):
            self.select_childrens_parents = True
            node_spec = node_spec[1:]

        if node_spec.startswith(SELECTOR_PARENTS):
            self.select_parents = True
            node_spec = node_spec[1:]

        if node_spec.endswith(SELECTOR_CHILDREN):
            self.select_children = True
            node_spec = node_spec[:-1]

        if self.select_children and self.select_childrens_parents:
            raise RuntimeException(
                'Invalid node spec {} - "@" prefix and "+" suffix are '
                'incompatible'.format(self.raw)
            )

        if SELECTOR_DELIMITER in node_spec:
            selector_parts = node_spec.split(SELECTOR_DELIMITER, 1)
            selector_type, self.selector_value = selector_parts
            self.selector_type = SELECTOR_FILTERS(selector_type)
        else:
            self.selector_value = node_spec
            # if the selector type has an OS path separator in it, it can't
            # really be a valid file name, so assume it's a path.
            if _probably_path(node_spec):
                self.selector_type = SELECTOR_FILTERS.PATH
            else:
                self.selector_type = SELECTOR_FILTERS.FQN


def split_intersection_blocks(spec):
    return spec.split(INTERSECTION_DELIMITER)


class SELECTOR_FILTERS(str, Enum):
    FQN = 'fqn'
    TAG = 'tag'
    SOURCE = 'source'
    PATH = 'path'

    def __str__(self):
        return self._value_


def alert_non_existence(raw_spec, nodes):
    if len(nodes) == 0:
        warn_or_error(
            f"The selector '{str(raw_spec)}' does not match any nodes and will"
            f" be ignored"
        )


def split_specs(node_specs: Iterable[str]):
    specs: Set[str] = set()
    for spec in node_specs:
        parts = spec.split(SPEC_DELIMITER)
        specs.update(parts)

    return specs


def get_package_names(nodes):
    return set([node.split(".")[1] for node in nodes])


def is_selected_node(real_node, node_selector):
    for i, selector_part in enumerate(node_selector):

        is_last = (i == len(node_selector) - 1)

        # if we hit a GLOB, then this node is selected
        if selector_part == SELECTOR_GLOB:
            return True

        # match package.node_name or package.dir.node_name
        elif is_last and selector_part == real_node[-1]:
            return True

        elif len(real_node) <= i:
            return False

        elif real_node[i] == selector_part:
            continue

        else:
            return False

    # if we get all the way down here, then the node is a match
    return True


def _node_is_match(
    qualified_name: List[str], package_names: Set[str], fqn: List[str]
) -> bool:
    """Determine if a qualfied name matches an fqn, given the set of package
    names in the graph.

    :param List[str] qualified_name: The components of the selector or node
        name, split on '.'.
    :param Set[str] package_names: The set of pacakge names in the graph.
    :param List[str] fqn: The node's fully qualified name in the graph.
    """
    if len(qualified_name) == 1 and fqn[-1] == qualified_name[0]:
        return True

    if qualified_name[0] in package_names:
        if is_selected_node(fqn, qualified_name):
            return True

    for package_name in package_names:
        local_qualified_node_name = [package_name] + qualified_name
        if is_selected_node(fqn, local_qualified_node_name):
            return True

    return False


class ManifestSelector:
    FILTER: str

    def __init__(self, manifest):
        self.manifest = manifest

    def _node_iterator(
        self,
        included_nodes: Set[str],
        exclude: Optional[Container[str]],
        include: Optional[Container[str]],
    ) -> Iterable[Tuple[str, str]]:
        for unique_id, node in self.manifest.nodes.items():
            if unique_id not in included_nodes:
                continue
            if include is not None and node.resource_type not in include:
                continue
            if exclude is not None and node.resource_type in exclude:
                continue
            yield unique_id, node

    def parsed_nodes(self, included_nodes):
        for unique_id, node in self.manifest.nodes.items():
            if unique_id not in included_nodes:
                continue
            yield unique_id, node

    def source_nodes(self, included_nodes):
        for unique_id, source in self.manifest.sources.items():
            if unique_id not in included_nodes:
                continue
            yield unique_id, source

    def search(self, included_nodes, selector):
        raise NotImplementedError('subclasses should implement this')


class QualifiedNameSelector(ManifestSelector):
    FILTER = SELECTOR_FILTERS.FQN

    def search(self, included_nodes, selector):
        """Yield all nodes in the graph that match the selector.

        :param str selector: The selector or node name
        """
        qualified_name = selector.split(".")
        package_names = get_package_names(included_nodes)
        for node, real_node in self.parsed_nodes(included_nodes):
            if _node_is_match(qualified_name, package_names, real_node.fqn):
                yield node


class TagSelector(ManifestSelector):
    FILTER = SELECTOR_FILTERS.TAG

    def search(self, included_nodes, selector):
        """ yields nodes from graph that have the specified tag """
        search = chain(self.parsed_nodes(included_nodes),
                       self.source_nodes(included_nodes))
        for node, real_node in search:
            if selector in real_node.tags:
                yield node


class SourceSelector(ManifestSelector):
    FILTER = SELECTOR_FILTERS.SOURCE

    def search(self, included_nodes, selector):
        """yields nodes from graph are the specified source."""
        parts = selector.split('.')
        target_package = SELECTOR_GLOB
        if len(parts) == 1:
            target_source, target_table = parts[0], None
        elif len(parts) == 2:
            target_source, target_table = parts
        elif len(parts) == 3:
            target_package, target_source, target_table = parts
        else:  # len(parts) > 3 or len(parts) == 0
            msg = (
                'Invalid source selector value "{}". Sources must be of the '
                'form `${{source_name}}`, '
                '`${{source_name}}.${{target_name}}`, or '
                '`${{package_name}}.${{source_name}}.${{target_name}}'
            ).format(selector)
            raise RuntimeException(msg)

        for node, real_node in self.source_nodes(included_nodes):
            if target_package not in (real_node.package_name, SELECTOR_GLOB):
                continue
            if target_source not in (real_node.source_name, SELECTOR_GLOB):
                continue
            if target_table in (None, real_node.name, SELECTOR_GLOB):
                yield node


class PathSelector(ManifestSelector):
    FILTER = SELECTOR_FILTERS.PATH

    def search(self, included_nodes, selector):
        """Yield all nodes in the graph that match the given path.

        :param str selector: The path selector
        """
        # use '.' and not 'root' for easy comparison
        root = Path.cwd()
        paths = set(p.relative_to(root) for p in root.glob(selector))
        search = chain(self.parsed_nodes(included_nodes),
                       self.source_nodes(included_nodes))
        for node, real_node in search:
            if Path(real_node.root_path) != root:
                continue
            ofp = Path(real_node.original_file_path)
            if ofp in paths:
                yield node
            elif any(parent in paths for parent in ofp.parents):
                yield node


class InvalidSelectorError(Exception):
    pass


ValidSelector = Union[QualifiedNameSelector, TagSelector, SourceSelector]


class MultiSelector:
    """The base class of the node selector. It only about the manifest and
    selector types, including the glob operator, but does not handle any graph
    related behavior.
    """
    SELECTORS = [
        QualifiedNameSelector,
        TagSelector,
        SourceSelector,
        PathSelector,
    ]

    def __init__(self, manifest):
        self.manifest = manifest

    def get_selector(
        self, selector_type: str
    ):
        for cls in self.SELECTORS:
            if cls.FILTER == selector_type:
                return cls(self.manifest)

        raise InvalidSelectorError(selector_type)

    def select_included(self, included_nodes, selector_type, selector_value):
        selector = self.get_selector(selector_type)
        return set(selector.search(included_nodes, selector_value))


class Graph:
    """A wrapper around the networkx graph that understands SelectionCriteria
    and how they interact with the graph.
    """
    def __init__(self, graph):
        self.graph = graph

    def nodes(self):
        return set(self.graph.nodes())

    def __iter__(self):
        return iter(self.graph.nodes())

    def select_childrens_parents(self, selected: Set[str]) -> Set[str]:
        ancestors_for = self.select_children(selected) | selected
        return self.select_parents(ancestors_for) | ancestors_for

    def select_children(self, selected: Set[str]) -> Set[str]:
        descendants: Set[str] = set()
        for node in selected:
            descendants.update(nx.descendants(self.graph, node))
        return descendants

    def select_parents(self, selected: Set[str]) -> Set[str]:
        ancestors: Set[str] = set()
        for node in selected:
            ancestors.update(nx.ancestors(self.graph, node))
        return ancestors

    def select_successors(self, selected: Set[str]) -> Set[str]:
        successors: Set[str] = set()
        for node in selected:
            successors.update(self.graph.successors(node))
        return successors

    def collect_models(
        self, selected: Set[str], spec: SelectionCriteria,
    ) -> Set[str]:
        additional: Set[str] = set()
        if spec.select_childrens_parents:
            additional.update(self.select_childrens_parents(selected))
        if spec.select_parents:
            additional.update(self.select_parents(selected))
        if spec.select_children:
            additional.update(self.select_children(selected))
        return additional

    def subgraph(self, nodes: Iterable[str]) -> 'Graph':
        cls = type(self)
        return cls(self.graph.subgraph(nodes))


class NodeSelector(MultiSelector):
    def __init__(self, graph, manifest):
        self.full_graph = Graph(graph)
        super().__init__(manifest)

    def get_nodes_from_spec(self, graph, spec):
        try:
            collected = self.select_included(graph.nodes(),
                                             spec.selector_type,
                                             spec.selector_value)
        except InvalidSelectorError:
            valid_selectors = ", ".join(s.FILTER for s in self.SELECTORS)
            logger.info("The '{}' selector specified in {} is invalid. Must "
                        "be one of [{}]".format(
                            spec.selector_type,
                            spec.raw,
                            valid_selectors))
            return set()

        specified = graph.collect_models(collected, spec)
        collected.update(specified)

        tests = {
            n for n in graph.select_successors(collected)
            if self.manifest.nodes[n].resource_type == NodeType.Test
        }
        collected.update(tests)

        return collected

    def get_nodes_from_intersection_spec(self, graph, raw_spec):
        return set.intersection(
            *[self.get_nodes_from_spec(graph, SelectionCriteria(
                intersection_block_spec)) for intersection_block_spec in
              split_intersection_blocks(raw_spec)]
        )

    def get_nodes_from_multiple_specs(
            self,
            graph,
            specs,
            nodes=None,
            check_existence=False,
            exclude=False
    ):
        selected_nodes: Set[str] = coalesce(nodes, set())
        operator = set.difference_update if exclude else set.update

        for raw_spec in split_specs(specs):
            nodes = self.get_nodes_from_intersection_spec(graph, raw_spec)

            if check_existence:
                alert_non_existence(raw_spec, nodes)

            operator(selected_nodes, nodes)

        return selected_nodes

    def select_nodes(self, graph, raw_include_specs, raw_exclude_specs):
        raw_exclude_specs = coalesce(raw_exclude_specs, [])
        check_existence = True

        if not raw_include_specs:
            check_existence = False
            raw_include_specs = ['fqn:*', 'source:*']

        selected_nodes = self.get_nodes_from_multiple_specs(
            graph,
            raw_include_specs,
            check_existence=check_existence
        )
        selected_nodes = self.get_nodes_from_multiple_specs(
            graph,
            raw_exclude_specs,
            nodes=selected_nodes,
            exclude=True
        )

        return selected_nodes

    def _is_graph_member(self, node_name):
        if node_name in self.manifest.sources:
            return True
        node = self.manifest.nodes[node_name]
        return not node.empty and node.config.enabled

    def _is_match(self, node_name, resource_types, tags, required):
        if node_name in self.manifest.nodes:
            node = self.manifest.nodes[node_name]
        elif node_name in self.manifest.sources:
            node = self.manifest.sources[node_name]
        else:
            raise InternalException(
                f'Node {node_name} not found in the manifest!'
            )
        if node.resource_type not in resource_types:
            return False
        tags = set(tags)
        if tags and not bool(set(node.tags) & tags):
            # there are tags specified but none match
            return False
        for attr in required:
            if not getattr(node, attr):
                return False
        return True

    def get_selected(self, include, exclude, resource_types, tags, required):
        tags = coalesce(tags, [])

        graph_members = {
            node_name for node_name in self.full_graph.nodes()
            if self._is_graph_member(node_name)
        }
        filtered_graph = self.full_graph.subgraph(graph_members)
        selected_nodes = self.select_nodes(filtered_graph, include, exclude)

        filtered_nodes = set()
        for node_name in selected_nodes:
            if self._is_match(node_name, resource_types, tags, required):
                filtered_nodes.add(node_name)

        return filtered_nodes

    def select(self, query):
        include = query.get('include')
        exclude = query.get('exclude')
        resource_types = query.get('resource_types')
        tags = query.get('tags')
        required = query.get('required', ())
        addin_ephemeral_nodes = query.get('addin_ephemeral_nodes', True)

        selected = self.get_selected(include, exclude, resource_types, tags,
                                     required)

        # if you haven't selected any nodes, return that so we can give the
        # nice "no models selected" message.
        if not selected:
            return selected

        # we used to carefully go through all node ancestors and add those if
        # they were ephemeral. Sadly, the algorithm we used ended up being
        # O(n^2). Instead, since ephemeral nodes are almost free, just add all
        # ephemeral nodes in the graph.
        # someday at large enough scale we might want to prune it to only be
        # ancestors of the selected nodes so we can skip the compile.
        if addin_ephemeral_nodes:
            addins = {
                uid for uid, node in self.manifest.nodes.items()
                if node.is_ephemeral_model
            }
        else:
            addins = set()

        return selected | addins
