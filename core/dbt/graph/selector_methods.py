import abc
from itertools import chain
from pathlib import Path
from typing import Set, List

from hologram.helpers import StrEnum

from dbt.exceptions import RuntimeException


SELECTOR_GLOB = '*'
SELECTOR_DELIMITER = ':'


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


class SelectorMethod(metaclass=abc.ABCMeta):
    def __init__(self, manifest):
        self.manifest = manifest

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

    @abc.abstractmethod
    def search(self, included_nodes: Set[str], selector: str):
        raise NotImplementedError('subclasses should implement this')


class QualifiedNameSelectorMethod(SelectorMethod):
    def node_is_match(
        self,
        qualified_name: List[str],
        package_names: Set[str],
        fqn: List[str],
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

    def search(self, included_nodes, selector):
        """Yield all nodes in the graph that match the selector.

        :param str selector: The selector or node name
        """
        qualified_name = selector.split(".")
        parsed_nodes = list(self.parsed_nodes(included_nodes))
        package_names = {n.package_name for _, n in parsed_nodes}
        for node, real_node in parsed_nodes:
            if self.node_is_match(
                qualified_name,
                package_names,
                real_node.fqn,
            ):
                yield node


class TagSelectorMethod(SelectorMethod):
    def search(self, included_nodes, selector):
        """ yields nodes from graph that have the specified tag """
        search = chain(self.parsed_nodes(included_nodes),
                       self.source_nodes(included_nodes))
        for node, real_node in search:
            if selector in real_node.tags:
                yield node


class SourceSelectorMethod(SelectorMethod):
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


class PathSelectorMethod(SelectorMethod):
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


class MethodName(StrEnum):
    FQN = 'fqn'
    Tag = 'tag'
    Source = 'source'
    Path = 'path'
