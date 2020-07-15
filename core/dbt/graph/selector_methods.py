import abc
from itertools import chain
from pathlib import Path
from typing import Set, List, Dict, Iterator, Tuple, Any, Union, Type

from hologram.helpers import StrEnum

from .graph import UniqueId

from dbt.contracts.graph.compiled import (
    CompiledDataTestNode,
    CompiledSchemaTestNode,
    NonSourceNode,
)
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import (
    HasTestMetadata,
    ParsedDataTestNode,
    ParsedSchemaTestNode,
    ParsedSourceDefinition,
)
from dbt.exceptions import (
    InternalException,
    RuntimeException,
)


SELECTOR_GLOB = '*'
SELECTOR_DELIMITER = ':'


class MethodName(StrEnum):
    FQN = 'fqn'
    Tag = 'tag'
    Source = 'source'
    Path = 'path'
    Package = 'package'
    Config = 'config'
    TestName = 'test_name'
    TestType = 'test_type'


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


SelectorTarget = Union[ParsedSourceDefinition, NonSourceNode]


class SelectorMethod(metaclass=abc.ABCMeta):
    def __init__(self, manifest: Manifest, arguments: List[str]):
        self.manifest: Manifest = manifest
        self.arguments: List[str] = arguments

    def parsed_nodes(
        self,
        included_nodes: Set[UniqueId]
    ) -> Iterator[Tuple[UniqueId, NonSourceNode]]:

        for key, node in self.manifest.nodes.items():
            unique_id = UniqueId(key)
            if unique_id not in included_nodes:
                continue
            yield unique_id, node

    def source_nodes(
        self,
        included_nodes: Set[UniqueId]
    ) -> Iterator[Tuple[UniqueId, ParsedSourceDefinition]]:

        for key, source in self.manifest.sources.items():
            unique_id = UniqueId(key)
            if unique_id not in included_nodes:
                continue
            yield unique_id, source

    def all_nodes(
        self,
        included_nodes: Set[UniqueId]
    ) -> Iterator[Tuple[UniqueId, SelectorTarget]]:
        yield from chain(self.parsed_nodes(included_nodes),
                         self.source_nodes(included_nodes))

    @abc.abstractmethod
    def search(
        self,
        included_nodes: Set[UniqueId],
        selector: str,
    ) -> Iterator[UniqueId]:
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

    def search(
        self, included_nodes: Set[UniqueId], selector: str
    ) -> Iterator[UniqueId]:
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
    def search(
        self, included_nodes: Set[UniqueId], selector: str
    ) -> Iterator[UniqueId]:
        """ yields nodes from included that have the specified tag """
        for node, real_node in self.all_nodes(included_nodes):
            if selector in real_node.tags:
                yield node


class SourceSelectorMethod(SelectorMethod):
    def search(
        self, included_nodes: Set[UniqueId], selector: str
    ) -> Iterator[UniqueId]:
        """yields nodes from included are the specified source."""
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
    def search(
        self, included_nodes: Set[UniqueId], selector: str
    ) -> Iterator[UniqueId]:
        """Yields nodes from inclucded that match the given path.

        """
        # use '.' and not 'root' for easy comparison
        root = Path.cwd()
        paths = set(p.relative_to(root) for p in root.glob(selector))
        for node, real_node in self.all_nodes(included_nodes):
            if Path(real_node.root_path) != root:
                continue
            ofp = Path(real_node.original_file_path)
            if ofp in paths:
                yield node
            elif any(parent in paths for parent in ofp.parents):
                yield node


class PackageSelectorMethod(SelectorMethod):
    def search(
        self, included_nodes: Set[UniqueId], selector: str
    ) -> Iterator[UniqueId]:
        """Yields nodes from included that have the specified package"""
        for node, real_node in self.all_nodes(included_nodes):
            if real_node.package_name == selector:
                yield node


def _getattr_descend(obj: Any, attrs: List[str]) -> Any:
    value = obj
    for attr in attrs:
        value = getattr(value, attr)
    return value


class CaseInsensitive(str):
    def __eq__(self, other):
        if isinstance(other, str):
            return self.upper() == other.upper()
        else:
            return self.upper() == other


class ConfigSelectorMethod(SelectorMethod):
    def search(
        self, included_nodes: Set[UniqueId], selector: str
    ) -> Iterator[UniqueId]:
        parts = self.arguments
        # special case: if the user wanted to compoare test severity,
        # make the comparison case-insensitive
        if parts == ['severity']:
            selector = CaseInsensitive(selector)

        # search sources is kind of useless now source configs only have
        # 'enabled', which you can't really filter on anyway, but maybe we'll
        # add more someday, so search them anyway.
        for node, real_node in self.all_nodes(included_nodes):
            try:
                value = _getattr_descend(real_node.config, parts)
            except AttributeError:
                continue
            else:
                # the selector can only be a str, so call str() on the value.
                # of course, if one wished to render the selector in the jinja
                # native env, this would no longer be true

                if selector == str(value):
                    yield node


class TestNameSelectorMethod(SelectorMethod):
    def search(
        self, included_nodes: Set[UniqueId], selector: str
    ) -> Iterator[UniqueId]:
        for node, real_node in self.parsed_nodes(included_nodes):
            if isinstance(real_node, HasTestMetadata):
                if real_node.test_metadata.name == selector:
                    yield node


class TestTypeSelectorMethod(SelectorMethod):
    def search(
        self, included_nodes: Set[UniqueId], selector: str
    ) -> Iterator[UniqueId]:
        search_types: Tuple[Type, ...]
        if selector == 'schema':
            search_types = (ParsedSchemaTestNode, CompiledSchemaTestNode)
        elif selector == 'data':
            search_types = (ParsedDataTestNode, CompiledDataTestNode)
        else:
            raise RuntimeException(
                f'Invalid test type selector {selector}: expected "data" or '
                '"schema"'
            )

        for node, real_node in self.parsed_nodes(included_nodes):
            if isinstance(real_node, search_types):
                yield node


class MethodManager:
    SELECTOR_METHODS: Dict[MethodName, Type[SelectorMethod]] = {
        MethodName.FQN: QualifiedNameSelectorMethod,
        MethodName.Tag: TagSelectorMethod,
        MethodName.Source: SourceSelectorMethod,
        MethodName.Path: PathSelectorMethod,
        MethodName.Package: PackageSelectorMethod,
        MethodName.Config: ConfigSelectorMethod,
        MethodName.TestName: TestNameSelectorMethod,
        MethodName.TestType: TestTypeSelectorMethod,
    }

    def __init__(self, manifest: Manifest):
        self.manifest = manifest

    def get_method(
        self, method: MethodName, method_arguments: List[str]
    ) -> SelectorMethod:

        if method not in self.SELECTOR_METHODS:
            raise InternalException(
                f'Method name "{method}" is a valid node selection '
                f'method name, but it is not handled'
            )
        cls: Type[SelectorMethod] = self.SELECTOR_METHODS[method]
        return cls(self.manifest, method_arguments)
