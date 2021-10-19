import abc
from itertools import chain
from pathlib import Path
from typing import Set, List, Dict, Iterator, Tuple, Any, Union, Type, Optional

from dbt.dataclass_schema import StrEnum

from .graph import UniqueId

from dbt.contracts.graph.compiled import (
    CompiledSingularTestNode,
    CompiledGenericTestNode,
    CompileResultNode,
    ManifestNode,
)
from dbt.contracts.graph.manifest import Manifest, WritableManifest
from dbt.contracts.graph.parsed import (
    HasTestMetadata,
    ParsedSingularTestNode,
    ParsedExposure,
    ParsedGenericTestNode,
    ParsedSourceDefinition,
)
from dbt.contracts.state import PreviousState
from dbt.exceptions import (
    InternalException,
    RuntimeException,
)
from dbt.node_types import NodeType


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
    Exposure = 'exposure'
    Result = 'result'


def is_selected_node(fqn: List[str], node_selector: str):

    # If qualified_name exactly matches model name (fqn's leaf), return True
    if fqn[-1] == node_selector:
        return True
    # Flatten node parts. Dots in model names act as namespace separators
    flat_fqn = [item for segment in fqn for item in segment.split('.')]
    # Selector components cannot be more than fqn's
    if len(flat_fqn) < len(node_selector.split('.')):
        return False

    for i, selector_part in enumerate(node_selector.split('.')):
        # if we hit a GLOB, then this node is selected
        if selector_part == SELECTOR_GLOB:
            return True
        elif flat_fqn[i] == selector_part:
            continue
        else:
            return False

    # if we get all the way down here, then the node is a match
    return True


SelectorTarget = Union[ParsedSourceDefinition, ManifestNode, ParsedExposure]


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
    ) -> Iterator[Tuple[UniqueId, ManifestNode]]:

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

    def exposure_nodes(
        self,
        included_nodes: Set[UniqueId]
    ) -> Iterator[Tuple[UniqueId, ParsedExposure]]:

        for key, exposure in self.manifest.exposures.items():
            unique_id = UniqueId(key)
            if unique_id not in included_nodes:
                continue
            yield unique_id, exposure

    def all_nodes(
        self,
        included_nodes: Set[UniqueId]
    ) -> Iterator[Tuple[UniqueId, SelectorTarget]]:
        yield from chain(self.parsed_nodes(included_nodes),
                         self.source_nodes(included_nodes),
                         self.exposure_nodes(included_nodes))

    def configurable_nodes(
        self,
        included_nodes: Set[UniqueId]
    ) -> Iterator[Tuple[UniqueId, CompileResultNode]]:
        yield from chain(self.parsed_nodes(included_nodes),
                         self.source_nodes(included_nodes))

    def non_source_nodes(
        self,
        included_nodes: Set[UniqueId],
    ) -> Iterator[Tuple[UniqueId, Union[ParsedExposure, ManifestNode]]]:
        yield from chain(self.parsed_nodes(included_nodes),
                         self.exposure_nodes(included_nodes))

    @abc.abstractmethod
    def search(
        self,
        included_nodes: Set[UniqueId],
        selector: str,
    ) -> Iterator[UniqueId]:
        raise NotImplementedError('subclasses should implement this')


class QualifiedNameSelectorMethod(SelectorMethod):
    def node_is_match(self, qualified_name: str, fqn: List[str]) -> bool:
        """Determine if a qualified name matches an fqn for all package
        names in the graph.

        :param str qualified_name: The qualified name to match the nodes with
        :param List[str] fqn: The node's fully qualified name in the graph.
        """
        unscoped_fqn = fqn[1:]

        if is_selected_node(fqn, qualified_name):
            return True
        # Match nodes across different packages
        elif is_selected_node(unscoped_fqn, qualified_name):
            return True

        return False

    def search(
        self, included_nodes: Set[UniqueId], selector: str
    ) -> Iterator[UniqueId]:
        """Yield all nodes in the graph that match the selector.

        :param str selector: The selector or node name
        """
        parsed_nodes = list(self.parsed_nodes(included_nodes))
        for node, real_node in parsed_nodes:
            if self.node_is_match(selector, real_node.fqn):
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
            if target_table not in (None, real_node.name, SELECTOR_GLOB):
                continue

            yield node


class ExposureSelectorMethod(SelectorMethod):
    def search(
        self, included_nodes: Set[UniqueId], selector: str
    ) -> Iterator[UniqueId]:
        parts = selector.split('.')
        target_package = SELECTOR_GLOB
        if len(parts) == 1:
            target_name = parts[0]
        elif len(parts) == 2:
            target_package, target_name = parts
        else:
            msg = (
                'Invalid exposure selector value "{}". Exposures must be of '
                'the form ${{exposure_name}} or '
                '${{exposure_package.exposure_name}}'
            ).format(selector)
            raise RuntimeException(msg)

        for node, real_node in self.exposure_nodes(included_nodes):
            if target_package not in (real_node.package_name, SELECTOR_GLOB):
                continue
            if target_name not in (real_node.name, SELECTOR_GLOB):
                continue

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
        for node, real_node in self.configurable_nodes(included_nodes):
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
        # continue supporting 'schema' + 'data' for backwards compatibility
        if selector in ('generic', 'schema'):
            search_types = (ParsedGenericTestNode, CompiledGenericTestNode)
        elif selector in ('singular', 'data'):
            search_types = (ParsedSingularTestNode, CompiledSingularTestNode)
        else:
            raise RuntimeException(
                f'Invalid test type selector {selector}: expected "generic" or '
                '"singular"'
            )

        for node, real_node in self.parsed_nodes(included_nodes):
            if isinstance(real_node, search_types):
                yield node


class StateSelectorMethod(SelectorMethod):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.modified_macros: Optional[List[str]] = None

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
            if uid in old_macros:
                old_macro = old_macros[uid]
                if macro.macro_sql != old_macro.macro_sql:
                    modified.append(uid)
            else:
                modified.append(uid)

        for uid, macro in old_macros.items():
            if uid not in new_macros:
                modified.append(uid)

        return modified

    def recursively_check_macros_modified(self, node, previous_macros):
        # loop through all macros that this node depends on
        for macro_uid in node.depends_on.macros:
            # avoid infinite recursion if we've already seen this macro
            if macro_uid in previous_macros:
                continue
            previous_macros.append(macro_uid)
            # is this macro one of the modified macros?
            if macro_uid in self.modified_macros:
                return True
            # if not, and this macro depends on other macros, keep looping
            macro_node = self.manifest.macros[macro_uid]
            if len(macro_node.depends_on.macros) > 0:
                return self.recursively_check_macros_modified(macro_node, previous_macros)
            else:
                return False

    def check_macros_modified(self, node):
        # check if there are any changes in macros the first time
        if self.modified_macros is None:
            self.modified_macros = self._macros_modified()
        # no macros have been modified, skip looping entirely
        if not self.modified_macros:
            return False
        # recursively loop through upstream macros to see if any is modified
        else:
            previous_macros = []
            return self.recursively_check_macros_modified(node, previous_macros)

    def check_modified(self, old: Optional[SelectorTarget], new: SelectorTarget) -> bool:
        different_contents = not new.same_contents(old)  # type: ignore
        upstream_macro_change = self.check_macros_modified(new)
        return different_contents or upstream_macro_change

    def check_modified_body(self, old: Optional[SelectorTarget], new: SelectorTarget) -> bool:
        if hasattr(new, "same_body"):
            return not new.same_body(old)  # type: ignore
        else:
            return False

    def check_modified_configs(self, old: Optional[SelectorTarget], new: SelectorTarget) -> bool:
        if hasattr(new, "same_config"):
            return not new.same_config(old)  # type: ignore
        else:
            return False

    def check_modified_persisted_descriptions(
        self, old: Optional[SelectorTarget], new: SelectorTarget
    ) -> bool:
        if hasattr(new, "same_persisted_description"):
            return not new.same_persisted_description(old)  # type: ignore
        else:
            return False

    def check_modified_relation(
        self, old: Optional[SelectorTarget], new: SelectorTarget
    ) -> bool:
        if hasattr(new, "same_database_representation"):
            return not new.same_database_representation(old)  # type: ignore
        else:
            return False

    def check_modified_macros(self, _, new: SelectorTarget) -> bool:
        return self.check_macros_modified(new)

    def check_new(self, old: Optional[SelectorTarget], new: SelectorTarget) -> bool:
        return old is None

    def search(
        self, included_nodes: Set[UniqueId], selector: str
    ) -> Iterator[UniqueId]:
        if self.previous_state is None or self.previous_state.manifest is None:
            raise RuntimeException(
                'Got a state selector method, but no comparison manifest'
            )

        state_checks = {
            # it's new if there is no old version
            'new': lambda old, _: old is None,
            # use methods defined above to compare properties of old + new
            'modified': self.check_modified,
            'modified.body': self.check_modified_body,
            'modified.configs': self.check_modified_configs,
            'modified.persisted_descriptions': self.check_modified_persisted_descriptions,
            'modified.relation': self.check_modified_relation,
            'modified.macros': self.check_modified_macros,
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
            elif node in manifest.exposures:
                previous_node = manifest.exposures[node]

            if checker(previous_node, real_node):
                yield node


class ResultSelectorMethod(SelectorMethod):
    def search(
        self, included_nodes: Set[UniqueId], selector: str
    ) -> Iterator[UniqueId]:
        if self.previous_state is None or self.previous_state.results is None:
            raise InternalException(
                'No comparison run_results'
            )
        matches = set(
            result.unique_id for result in self.previous_state.results
            if result.status == selector
        )
        for node, real_node in self.all_nodes(included_nodes):
            if node in matches:
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
        MethodName.Exposure: ExposureSelectorMethod,
        MethodName.Result: ResultSelectorMethod,
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
