import networkx as nx
from dbt.logger import GLOBAL_LOGGER as logger

from dbt.utils import is_enabled, get_materialization, coalesce
from dbt.node_types import NodeType
from dbt.contracts.graph.parsed import ParsedNode
import dbt.exceptions

SELECTOR_PARENTS = '+'
SELECTOR_CHILDREN = '+'
SELECTOR_GLOB = '*'
SELECTOR_DELIMITER = ':'


class SELECTOR_FILTERS(object):
    FQN = 'fqn'
    TAG = 'tag'


def split_specs(node_specs):
    specs = set()
    for spec in node_specs:
        parts = spec.split(" ")
        specs.update(parts)

    return specs


def parse_spec(node_spec):
    select_children = False
    select_parents = False
    index_start = 0
    index_end = len(node_spec)

    if node_spec.startswith(SELECTOR_PARENTS):
        select_parents = True
        index_start = 1

    if node_spec.endswith(SELECTOR_CHILDREN):
        select_children = True
        index_end -= 1

    node_selector = node_spec[index_start:index_end]

    if SELECTOR_DELIMITER in node_selector:
        selector_parts = node_selector.split(SELECTOR_DELIMITER, 1)
        selector_type, selector_value = selector_parts

        node_filter = {
            "type": selector_type,
            "value": selector_value
        }

    else:
        node_filter = {
            "type": SELECTOR_FILTERS.FQN,
            "value": node_selector

        }

    return {
        "select_parents": select_parents,
        "select_children": select_children,
        "filter": node_filter,
        "raw": node_spec
    }


def get_package_names(graph):
    return set([node.split(".")[1] for node in graph.nodes()])


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


def _node_is_match(qualified_name, package_names, fqn):
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


def get_nodes_by_qualified_name(graph, qualified_name_selector):
    """Yield all nodes in the graph that match the qualified_name_selector.

    :param str qualified_name_selector: The selector or node name
    """
    qualified_name = qualified_name_selector.split(".")
    package_names = get_package_names(graph)

    for node in graph.nodes():
        fqn_ish = graph.node[node]['fqn']
        if _node_is_match(qualified_name, package_names, fqn_ish):
            yield node


def get_nodes_by_tag(graph, tag_name):
    """ yields nodes from graph that have the specified tag """

    for node in graph.nodes():
        tags = graph.node[node]['tags']

        if tag_name in tags:
            yield node


def get_nodes_from_spec(graph, spec):
    select_parents = spec['select_parents']
    select_children = spec['select_children']

    filter_map = {
        SELECTOR_FILTERS.FQN: get_nodes_by_qualified_name,
        SELECTOR_FILTERS.TAG: get_nodes_by_tag,
    }

    node_filter = spec['filter']
    filter_func = filter_map.get(node_filter['type'])

    if filter_func is None:
        valid_selectors = ", ".join(filter_map.keys())
        logger.info("The '{}' selector specified in {} is invalid. Must be "
                    "one of [{}]".format(
                        node_filter['type'],
                        spec['raw'],
                        valid_selectors))

        selected_nodes = set()

    else:
        selected_nodes = set(filter_func(graph, node_filter['value']))

    additional_nodes = set()
    test_nodes = set()

    if select_parents:
        for node in selected_nodes:
            parent_nodes = nx.ancestors(graph, node)
            additional_nodes.update(parent_nodes)

    if select_children:
        for node in selected_nodes:
            child_nodes = nx.descendants(graph, node)
            additional_nodes.update(child_nodes)

    model_nodes = selected_nodes | additional_nodes

    for node in model_nodes:
        # include tests that depend on this node. if we aren't running tests,
        # they'll be filtered out later.
        child_tests = [n for n in graph.successors(node)
                       if graph.node.get(n).get('resource_type') ==
                       NodeType.Test]
        test_nodes.update(child_tests)

    return model_nodes | test_nodes


def warn_if_useless_spec(spec, nodes):
    if len(nodes) > 0:
        return

    logger.info(
        "* Spec='{}' does not identify any models and was ignored\n"
        .format(spec['raw'])
    )


def select_nodes(graph, raw_include_specs, raw_exclude_specs):
    selected_nodes = set()

    split_include_specs = split_specs(raw_include_specs)
    split_exclude_specs = split_specs(raw_exclude_specs)

    include_specs = [parse_spec(spec) for spec in split_include_specs]
    exclude_specs = [parse_spec(spec) for spec in split_exclude_specs]

    for spec in include_specs:
        included_nodes = get_nodes_from_spec(graph, spec)
        warn_if_useless_spec(spec, included_nodes)
        selected_nodes = selected_nodes | included_nodes

    for spec in exclude_specs:
        excluded_nodes = get_nodes_from_spec(graph, spec)
        warn_if_useless_spec(spec, excluded_nodes)
        selected_nodes = selected_nodes - excluded_nodes

    return selected_nodes


class NodeSelector(object):
    def __init__(self, linker, manifest):
        self.linker = linker
        self.manifest = manifest

    def get_valid_nodes(self, graph):
        valid = []
        for node_name in graph.nodes():
            node = graph.node.get(node_name)

            if not node.get('empty') and is_enabled(node):
                valid.append(node_name)
        return valid

    def get_selected(self, include, exclude, resource_types, tags):
        graph = self.linker.graph

        include = coalesce(include, ['*'])
        exclude = coalesce(exclude, [])
        tags = coalesce(tags, [])

        to_run = self.get_valid_nodes(graph)
        filtered_graph = graph.subgraph(to_run)
        selected_nodes = select_nodes(filtered_graph, include, exclude)

        filtered_nodes = set()
        for node_name in selected_nodes:
            node = graph.node.get(node_name)

            matched_resource = node.get('resource_type') in resource_types
            matched_tags = (len(tags) == 0 or
                            bool(set(node.get('tags', [])) & set(tags)))

            if matched_resource and matched_tags:
                filtered_nodes.add(node_name)

        return filtered_nodes

    def is_ephemeral_model(self, node):
        is_model = node.get('resource_type') == NodeType.Model
        is_ephemeral = get_materialization(node) == 'ephemeral'
        return is_model and is_ephemeral

    def get_ancestor_ephemeral_nodes(self, selected_nodes):

        node_names = {
            node: self.manifest.nodes.get(node).name
            for node in selected_nodes
            if node in self.manifest.nodes
        }

        include_spec = [
            '+{}'.format(node_names[node])
            for node in selected_nodes if node in node_names
        ]

        all_ancestors = select_nodes(self.linker.graph, include_spec, [])

        res = []
        for ancestor in all_ancestors:
            ancestor_node = self.manifest.nodes.get(ancestor, None)

            if ancestor_node and self.is_ephemeral_model(ancestor_node):
                res.append(ancestor)

        return set(res)

    def select(self, query):
        include = query.get('include')
        exclude = query.get('exclude')
        resource_types = query.get('resource_types')
        tags = query.get('tags')

        selected = self.get_selected(include, exclude, resource_types, tags)
        addins = self.get_ancestor_ephemeral_nodes(selected)

        return selected | addins

    def as_node_list(self, selected_nodes, ephemeral_only=False):
        dependency_list = self.linker.as_dependency_list(
            selected_nodes,
            ephemeral_only=ephemeral_only)

        concurrent_dependency_list = []
        for level in dependency_list:
            node_level = [
                ParsedNode(**self.linker.get_node(node)) for node in level
            ]
            concurrent_dependency_list.append(node_level)

        return concurrent_dependency_list


class FlatNodeSelector(NodeSelector):
    def as_node_list(self, selected_nodes):
        return super(FlatNodeSelector, self).as_node_list(selected_nodes,
                                                          ephemeral_only=True)
