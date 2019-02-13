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
    SOURCE = 'source'


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


def warn_if_useless_spec(spec, nodes):
    if len(nodes) > 0:
        return

    msg = (
        "* Spec='{}' does not identify any models"
        .format(spec['raw'])
    )
    dbt.exceptions.warn_or_error(msg, log_fmt='{} and was ignored\n')


class NodeSelector(object):
    def __init__(self, linker, manifest):
        self.linker = linker
        self.manifest = manifest

    def _node_iterator(self, graph, exclude, include):
        for node in graph.nodes():
            real_node = self.manifest.nodes[node]
            if include is not None and real_node.resource_type not in include:
                continue
            if exclude is not None and real_node.resource_type in exclude:
                continue
            yield node, real_node

    def parsed_nodes(self, graph):
        return self._node_iterator(
            graph,
            exclude=(NodeType.Source,),
            include=None)

    def source_nodes(self, graph):
        return self._node_iterator(
            graph,
            exclude=None,
            include=(NodeType.Source,))

    def get_nodes_by_qualified_name(self, graph, qualified_name_selector):
        """Yield all nodes in the graph that match the qualified_name_selector.

        :param str qualified_name_selector: The selector or node name
        """
        qualified_name = qualified_name_selector.split(".")
        package_names = get_package_names(graph)
        for node, real_node in self.parsed_nodes(graph):
            if _node_is_match(qualified_name, package_names, real_node.fqn):
                yield node

    def get_nodes_by_tag(self, graph, tag_name):
        """ yields nodes from graph that have the specified tag """
        for node, real_node in self.parsed_nodes(graph):
            if tag_name in real_node.tags:
                yield node

    def get_nodes_by_source(self, graph, source_full_name):
        """yields nodes from graph are the specified source."""
        parts = source_full_name.split('.')
        if len(parts) == 1:
            target_source, target_table = parts[0], None
        elif len(parts) == 2:
            target_source, target_table = parts
        else:  # len(parts) > 2 or len(parts) == 0
            msg = (
                'Invalid source selector value "{}". Sources must be of the '
                'form `${{source_name}}` or '
                '`${{source_name}}.${{target_name}}`'
            ).format(source_full_name)
            raise dbt.exceptions.RuntimeException(msg)

        for node, real_node in self.source_nodes(graph):
            if target_source not in (real_node.source_name, SELECTOR_GLOB):
                continue
            if target_table in (None, real_node.name, SELECTOR_GLOB):
                yield node

    def get_nodes_from_spec(self, graph, spec):
        select_parents = spec['select_parents']
        select_children = spec['select_children']

        filter_map = {
            SELECTOR_FILTERS.FQN: self.get_nodes_by_qualified_name,
            SELECTOR_FILTERS.TAG: self.get_nodes_by_tag,
            SELECTOR_FILTERS.SOURCE: self.get_nodes_by_source,
        }

        node_filter = spec['filter']
        filter_method = filter_map.get(node_filter['type'])

        if filter_method is None:
            valid_selectors = ", ".join(filter_map.keys())
            logger.info("The '{}' selector specified in {} is invalid. Must "
                        "be one of [{}]".format(
                            node_filter['type'],
                            spec['raw'],
                            valid_selectors))

            selected_nodes = set()

        else:
            selected_nodes = set(filter_method(graph, node_filter['value']))

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
            # include tests that depend on this node. if we aren't running
            # tests, they'll be filtered out later.
            child_tests = [n for n in graph.successors(node)
                           if self.manifest.nodes[n].resource_type ==
                           NodeType.Test]
            test_nodes.update(child_tests)

        return model_nodes | test_nodes

    def select_nodes(self, graph, raw_include_specs, raw_exclude_specs):
        selected_nodes = set()

        split_include_specs = split_specs(raw_include_specs)
        split_exclude_specs = split_specs(raw_exclude_specs)

        include_specs = [parse_spec(spec) for spec in split_include_specs]
        exclude_specs = [parse_spec(spec) for spec in split_exclude_specs]

        for spec in include_specs:
            included_nodes = self.get_nodes_from_spec(graph, spec)
            warn_if_useless_spec(spec, included_nodes)
            selected_nodes = selected_nodes | included_nodes

        for spec in exclude_specs:
            excluded_nodes = self.get_nodes_from_spec(graph, spec)
            warn_if_useless_spec(spec, excluded_nodes)
            selected_nodes = selected_nodes - excluded_nodes

        return selected_nodes

    def _is_graph_member(self, node_name):
        node = self.manifest.nodes[node_name]
        if node.resource_type == NodeType.Source:
            return True
        return not node.get('empty') and is_enabled(node)

    def get_valid_nodes(self, graph):
        return [
            node_name for node_name in graph.nodes()
            if self._is_graph_member(node_name)
        ]

    def _is_match(self, node_name, resource_types, tags, required):
        node = self.manifest.nodes[node_name]
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
        graph = self.linker.graph

        include = coalesce(include, ['*'])
        exclude = coalesce(exclude, [])
        tags = coalesce(tags, [])

        to_run = self.get_valid_nodes(graph)
        filtered_graph = graph.subgraph(to_run)
        selected_nodes = self.select_nodes(filtered_graph, include, exclude)

        filtered_nodes = set()
        for node_name in selected_nodes:
            if self._is_match(node_name, resource_types, tags, required):
                filtered_nodes.add(node_name)

        return filtered_nodes

    def is_ephemeral_model(self, node):
        is_model = node.get('resource_type') == NodeType.Model
        is_ephemeral = get_materialization(node) == 'ephemeral'
        return is_model and is_ephemeral

    def get_ancestor_ephemeral_nodes(self, selected_nodes):
        node_names = {}
        for node_id in selected_nodes:
            if node_id not in self.manifest.nodes:
                continue
            node = self.manifest.nodes[node_id]
            # sources don't have ancestors and this results in a silly select()
            if node.resource_type == NodeType.Source:
                continue
            node_names[node_id] = node.name

        include_spec = [
            '+{}'.format(node_names[node])
            for node in selected_nodes if node in node_names
        ]
        if not include_spec:
            return set()

        all_ancestors = self.select_nodes(self.linker.graph, include_spec, [])

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
        required = query.get('required', ())

        selected = self.get_selected(include, exclude, resource_types, tags,
                                     required)

        addins = self.get_ancestor_ephemeral_nodes(selected)

        return selected | addins
