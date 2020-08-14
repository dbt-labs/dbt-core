import abc
from itertools import chain
from pathlib import Path
from typing import Set, List, Dict, Iterator, Tuple, Any, Union, Type, Optional

from hologram.helpers import StrEnum

from .graph import UniqueId

from dbt.contracts.graph.compiled import (
    CompiledDataTestNode,
    CompiledSchemaTestNode,
    NonSourceNode,
)
from dbt.contracts.graph.manifest import Manifest, WritableManifest
from dbt.contracts.graph.parsed import (
    HasTestMetadata,
    ParsedDataTestNode,
    ParsedSchemaTestNode,
    ParsedSourceDefinition,
)
from dbt.contracts.state import PreviousState
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.exceptions import (
    InternalException,
    RuntimeException,
)
from dbt.node_types import NodeType
from dbt.ui import warning_tag


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
    ResourceType = 'resource_type'
    State = 'state'


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
    def __init__(
        self,
        manifest: Manifest,
        previous_state: Optional[PreviousState],
        arguments: List[str]
    ):
        self.manifest: Manifest = manifest
        self.previous_state = previous_state
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
        try:
            value = getattr(value, attr)
        except AttributeError:
            # if it implements getitem (dict, list, ...), use that. On failure,
            # raise an attribute error instead of the KeyError, TypeError, etc.
            # that arbitrary getitem calls might raise
            try:
                value = value[attr]
            except Exception as exc:
                raise AttributeError(
                    f"'{type(value)}' object has no attribute '{attr}'"
                ) from exc
    return value


class CaseInsensitive(str):
    def __eq__(self, other):
        if isinstance(other, str):
            return self.upper() == other.upper()
        else:
            return self.upper() == other


class ConfigSelectorMethod(SelectorMethod):
    def search(
        self,
        included_nodes: Set[UniqueId],
        selector: Any,
    ) -> Iterator[UniqueId]:
        parts = self.arguments
        # special case: if the user wanted to compare test severity,
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
                if selector == value:
                    yield node


class ResourceTypeSelectorMethod(SelectorMethod):
    def search(
        self, included_nodes: Set[UniqueId], selector: str
    ) -> Iterator[UniqueId]:
        try:
            resource_type = NodeType(selector)
        except ValueError as exc:
            raise RuntimeException(
                f'Invalid resource_type selector "{selector}"'
            ) from exc
        for node, real_node in self.parsed_nodes(included_nodes):
            if real_node.resource_type == resource_type:
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


class StateSelectorMethod(SelectorMethod):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.macros_were_modified: Optional[List[str]] = None

    def _macros_modified(self) -> List[str]:
        # we checked in the caller!
        if self.previous_state is None or self.previous_state.manifest is None:
            raise InternalException(
                'No comparison manifest in _macros_modified'
            )
        old_macros = self.previous_state.manifest.macros
        new_macros = self.manifest.macros

        modified = []
        for uid, macro in new_macros.items():
            name = f'{macro.package_name}.{macro.name}'
            if uid in old_macros:
                old_macro = old_macros[uid]
                if macro.macro_sql != old_macro.macro_sql:
                    modified.append(f'{name} changed')
            else:
                modified.append(f'{name} added')

        for uid, macro in old_macros.items():
            if uid not in new_macros:
                modified.append(f'{macro.package_name}.{macro.name} removed')

        return modified[:3]

    def check_modified(
        self,
        old: Optional[SelectorTarget],
        new: SelectorTarget,
    ) -> bool:
        # check if there are any changes in macros, if so, log a warning the
        # first time
        if self.macros_were_modified is None:
            self.macros_were_modified = self._macros_modified()
            if self.macros_were_modified:
                log_str = ', '.join(self.macros_were_modified)
                logger.warning(warning_tag(
                    f'During a state comparison, dbt detected a change in '
                    f'macros. This will not be marked as a modification. Some '
                    f'macros: {log_str}'
                ))

        return not new.same_contents(old)  # type: ignore

    def check_new(
        self,
        old: Optional[SelectorTarget],
        new: SelectorTarget,
    ) -> bool:
        return old is None

    def search(
        self, included_nodes: Set[UniqueId], selector: str
    ) -> Iterator[UniqueId]:
        if self.previous_state is None or self.previous_state.manifest is None:
            raise RuntimeException(
                'Got a state selector method, but no comparison manifest'
            )

        state_checks = {
            'modified': self.check_modified,
            'new': self.check_new,
        }
        if selector in state_checks:
            checker = state_checks[selector]
        else:
            raise RuntimeException(
                f'Got an invalid selector "{selector}", expected one of '
                f'"{list(state_checks)}"'
            )

        manifest: WritableManifest = self.previous_state.manifest

        for node, real_node in self.all_nodes(included_nodes):
            previous_node: Optional[SelectorTarget] = None
            if node in manifest.nodes:
                previous_node = manifest.nodes[node]
            elif node in manifest.sources:
                previous_node = manifest.sources[node]

            if checker(previous_node, real_node):
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
        MethodName.State: StateSelectorMethod,
    }

    def __init__(
        self,
        manifest: Manifest,
        previous_state: Optional[PreviousState],
    ):
        self.manifest = manifest
        self.previous_state = previous_state

    def get_method(
        self, method: MethodName, method_arguments: List[str]
    ) -> SelectorMethod:

        if method not in self.SELECTOR_METHODS:
            raise InternalException(
                f'Method name "{method}" is a valid node selection '
                f'method name, but it is not handled'
            )
        cls: Type[SelectorMethod] = self.SELECTOR_METHODS[method]
        return cls(self.manifest, self.previous_state, method_arguments)
