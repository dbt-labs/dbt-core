import abc
from itertools import chain
from pathlib import Path
from typing import Set, List, Dict, Iterator, Tuple, Any, Union, Type, Optional, Callable

from dbt.dataclass_schema import StrEnum

from .graph import UniqueId

from dbt.contracts.graph.manifest import Manifest, WritableManifest
from dbt.contracts.graph.nodes import (
    SingularTestNode,
    Exposure,
    Metric,
    GenericTestNode,
    SourceDefinition,
    ResultNode,
    ManifestNode,
)
from dbt.contracts.state import PreviousState
from dbt.exceptions import (
    DbtInternalError,
    DbtRuntimeError,
)
from dbt.node_types import NodeType


SELECTOR_GLOB = "*"
SELECTOR_DELIMITER = ":"


class MethodName(StrEnum):
    FQN = "fqn"
    Tag = "tag"
    Group = "group"
    Source = "source"
    Path = "path"
    File = "file"
    Package = "package"
    Config = "config"
    TestName = "test_name"
    TestType = "test_type"
    ResourceType = "resource_type"
    State = "state"
    Exposure = "exposure"
    Metric = "metric"
    Result = "result"
    SourceStatus = "source_status"


def is_selected_node(fqn: List[str], node_selector: str):

    # If qualified_name exactly matches model name (fqn's leaf), return True
    if fqn[-1] == node_selector:
        return True
    # Flatten node parts. Dots in model names act as namespace separators
    flat_fqn = [item for segment in fqn for item in segment.split(".")]
    # Selector components cannot be more than fqn's
    if len(flat_fqn) < len(node_selector.split(".")):
        return False

    for i, selector_part in enumerate(node_selector.split(".")):
        # if we hit a GLOB, then this node is selected
        if selector_part == SELECTOR_GLOB:
            return True
        elif flat_fqn[i] == selector_part:
            continue
        else:
            return False

    # if we get all the way down here, then the node is a match
    return True


SelectorTarget = Union[SourceDefinition, ManifestNode, Exposure, Metric]


class SelectorMethod(metaclass=abc.ABCMeta):
    def __init__(
        self, manifest: Manifest, previous_state: Optional[PreviousState], arguments: List[str]
    ):
        self.manifest: Manifest = manifest
        self.previous_state = previous_state
        self.arguments: List[str] = arguments

    def parsed_nodes(
        self, included_nodes: Set[UniqueId]
    ) -> Iterator[Tuple[UniqueId, ManifestNode]]:

        for key, node in self.manifest.nodes.items():
            unique_id = UniqueId(key)
            if unique_id not in included_nodes:
                continue
            yield unique_id, node

    def source_nodes(
        self, included_nodes: Set[UniqueId]
    ) -> Iterator[Tuple[UniqueId, SourceDefinition]]:

        for key, source in self.manifest.sources.items():
            unique_id = UniqueId(key)
            if unique_id not in included_nodes:
                continue
            yield unique_id, source

    def exposure_nodes(self, included_nodes: Set[UniqueId]) -> Iterator[Tuple[UniqueId, Exposure]]:

        for key, exposure in self.manifest.exposures.items():
            unique_id = UniqueId(key)
            if unique_id not in included_nodes:
                continue
            yield unique_id, exposure

    def metric_nodes(self, included_nodes: Set[UniqueId]) -> Iterator[Tuple[UniqueId, Metric]]:

        for key, metric in self.manifest.metrics.items():
            unique_id = UniqueId(key)
            if unique_id not in included_nodes:
                continue
            yield unique_id, metric

    def all_nodes(
        self, included_nodes: Set[UniqueId]
    ) -> Iterator[Tuple[UniqueId, SelectorTarget]]:
        yield from chain(
            self.parsed_nodes(included_nodes),
            self.source_nodes(included_nodes),
            self.exposure_nodes(included_nodes),
            self.metric_nodes(included_nodes),
        )

    def configurable_nodes(
        self, included_nodes: Set[UniqueId]
    ) -> Iterator[Tuple[UniqueId, ResultNode]]:
        yield from chain(self.parsed_nodes(included_nodes), self.source_nodes(included_nodes))

    def non_source_nodes(
        self,
        included_nodes: Set[UniqueId],
    ) -> Iterator[Tuple[UniqueId, Union[Exposure, ManifestNode, Metric]]]:
        yield from chain(
            self.parsed_nodes(included_nodes),
            self.exposure_nodes(included_nodes),
            self.metric_nodes(included_nodes),
        )

    def groupable_nodes(
        self,
        included_nodes: Set[UniqueId],
    ) -> Iterator[Tuple[UniqueId, Union[ManifestNode, Metric]]]:
        yield from chain(
            self.parsed_nodes(included_nodes),
            self.metric_nodes(included_nodes),
        )

    @abc.abstractmethod
    def search(
        self,
        included_nodes: Set[UniqueId],
        selector: str,
    ) -> Iterator[UniqueId]:
        raise NotImplementedError("subclasses should implement this")


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

    def search(self, included_nodes: Set[UniqueId], selector: str) -> Iterator[UniqueId]:
        """Yield all nodes in the graph that match the selector.

        :param str selector: The selector or node name
        """
        parsed_nodes = list(self.parsed_nodes(included_nodes))
        for node, real_node in parsed_nodes:
            if self.node_is_match(selector, real_node.fqn):
                yield node


class TagSelectorMethod(SelectorMethod):
    def search(self, included_nodes: Set[UniqueId], selector: str) -> Iterator[UniqueId]:
        """yields nodes from included that have the specified tag"""
        for node, real_node in self.all_nodes(included_nodes):
            if selector in real_node.tags:
                yield node


class GroupSelectorMethod(SelectorMethod):
    def search(self, included_nodes: Set[UniqueId], selector: str) -> Iterator[UniqueId]:
        """yields nodes from included in the specified group"""
        for node, real_node in self.groupable_nodes(included_nodes):
            if selector == real_node.config.get("group"):
                yield node


class SourceSelectorMethod(SelectorMethod):
    def search(self, included_nodes: Set[UniqueId], selector: str) -> Iterator[UniqueId]:
        """yields nodes from included are the specified source."""
        parts = selector.split(".")
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
                "form `${{source_name}}`, "
                "`${{source_name}}.${{target_name}}`, or "
                "`${{package_name}}.${{source_name}}.${{target_name}}"
            ).format(selector)
            raise DbtRuntimeError(msg)

        for node, real_node in self.source_nodes(included_nodes):
            if target_package not in (real_node.package_name, SELECTOR_GLOB):
                continue
            if target_source not in (real_node.source_name, SELECTOR_GLOB):
                continue
            if target_table not in (None, real_node.name, SELECTOR_GLOB):
                continue

            yield node


class ExposureSelectorMethod(SelectorMethod):
    def search(self, included_nodes: Set[UniqueId], selector: str) -> Iterator[UniqueId]:
        parts = selector.split(".")
        target_package = SELECTOR_GLOB
        if len(parts) == 1:
            target_name = parts[0]
        elif len(parts) == 2:
            target_package, target_name = parts
        else:
            msg = (
                'Invalid exposure selector value "{}". Exposures must be of '
                "the form ${{exposure_name}} or "
                "${{exposure_package.exposure_name}}"
            ).format(selector)
            raise DbtRuntimeError(msg)

        for node, real_node in self.exposure_nodes(included_nodes):
            if target_package not in (real_node.package_name, SELECTOR_GLOB):
                continue
            if target_name not in (real_node.name, SELECTOR_GLOB):
                continue

            yield node


class MetricSelectorMethod(SelectorMethod):
    def search(self, included_nodes: Set[UniqueId], selector: str) -> Iterator[UniqueId]:
        parts = selector.split(".")
        target_package = SELECTOR_GLOB
        if len(parts) == 1:
            target_name = parts[0]
        elif len(parts) == 2:
            target_package, target_name = parts
        else:
            msg = (
                'Invalid metric selector value "{}". Metrics must be of '
                "the form ${{metric_name}} or "
                "${{metric_package.metric_name}}"
            ).format(selector)
            raise DbtRuntimeError(msg)

        for node, real_node in self.metric_nodes(included_nodes):
            if target_package not in (real_node.package_name, SELECTOR_GLOB):
                continue
            if target_name not in (real_node.name, SELECTOR_GLOB):
                continue

            yield node


class PathSelectorMethod(SelectorMethod):
    def search(self, included_nodes: Set[UniqueId], selector: str) -> Iterator[UniqueId]:
        """Yields nodes from included that match the given path."""
        # use '.' and not 'root' for easy comparison
        root = Path.cwd()
        paths = set(p.relative_to(root) for p in root.glob(selector))
        for node, real_node in self.all_nodes(included_nodes):
            ofp = Path(real_node.original_file_path)
            if ofp in paths:
                yield node
            elif any(parent in paths for parent in ofp.parents):
                yield node


class FileSelectorMethod(SelectorMethod):
    def search(self, included_nodes: Set[UniqueId], selector: str) -> Iterator[UniqueId]:
        """Yields nodes from included that match the given file name."""
        for node, real_node in self.all_nodes(included_nodes):
            if Path(real_node.original_file_path).name == selector:
                yield node


class PackageSelectorMethod(SelectorMethod):
    def search(self, included_nodes: Set[UniqueId], selector: str) -> Iterator[UniqueId]:
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
                raise AttributeError(f"'{type(value)}' object has no attribute '{attr}'") from exc
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
        if parts == ["severity"]:
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
                if isinstance(value, list):
                    if (
                        (selector in value)
                        or (CaseInsensitive(selector) == "true" and True in value)
                        or (CaseInsensitive(selector) == "false" and False in value)
                    ):
                        yield node
                else:
                    if (
                        (selector == value)
                        or (CaseInsensitive(selector) == "true" and value is True)
                        or (CaseInsensitive(selector) == "false")
                        and value is False
                    ):
                        yield node


class ResourceTypeSelectorMethod(SelectorMethod):
    def search(self, included_nodes: Set[UniqueId], selector: str) -> Iterator[UniqueId]:
        try:
            resource_type = NodeType(selector)
        except ValueError as exc:
            raise DbtRuntimeError(f'Invalid resource_type selector "{selector}"') from exc
        for node, real_node in self.parsed_nodes(included_nodes):
            if real_node.resource_type == resource_type:
                yield node


class TestNameSelectorMethod(SelectorMethod):
    def search(self, included_nodes: Set[UniqueId], selector: str) -> Iterator[UniqueId]:
        for node, real_node in self.parsed_nodes(included_nodes):
            if real_node.resource_type == NodeType.Test and hasattr(real_node, "test_metadata"):
                if real_node.test_metadata.name == selector:  # type: ignore[union-attr]
                    yield node


class TestTypeSelectorMethod(SelectorMethod):
    def search(self, included_nodes: Set[UniqueId], selector: str) -> Iterator[UniqueId]:
        search_type: Type
        # continue supporting 'schema' + 'data' for backwards compatibility
        if selector in ("generic", "schema"):
            search_type = GenericTestNode
        elif selector in ("singular", "data"):
            search_type = SingularTestNode
        else:
            raise DbtRuntimeError(
                f'Invalid test type selector {selector}: expected "generic" or ' '"singular"'
            )

        for node, real_node in self.parsed_nodes(included_nodes):
            if isinstance(real_node, search_type):
                yield node


class StateSelectorMethod(SelectorMethod):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.modified_macros: Optional[List[str]] = None

    def _macros_modified(self) -> List[str]:
        # we checked in the caller!
        if self.previous_state is None or self.previous_state.manifest is None:
            raise DbtInternalError("No comparison manifest in _macros_modified")
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

    def recursively_check_macros_modified(self, node, visited_macros):
        if not hasattr(node, "depends_on"):
            return False

        for macro_uid in node.depends_on.macros:
            if macro_uid in visited_macros:
                continue
            visited_macros.append(macro_uid)

            if macro_uid in self.modified_macros:
                return True

            # this macro hasn't been modified, but depends on other
            # macros which each need to be tested for modification
            macro_node = self.manifest.macros[macro_uid]
            if len(macro_node.depends_on.macros) > 0:
                upstream_macros_changed = self.recursively_check_macros_modified(
                    macro_node, visited_macros
                )
                if upstream_macros_changed:
                    return True
                continue

            # this macro hasn't been modified, but we haven't checked
            # the other macros the node depends on, so keep looking
            if len(node.depends_on.macros) > len(visited_macros):
                continue

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
            visited_macros = []
            return self.recursively_check_macros_modified(node, visited_macros)

    # TODO check modifed_content and check_modified macro seems a bit redundent
    def check_modified_content(self, old: Optional[SelectorTarget], new: SelectorTarget) -> bool:
        different_contents = not new.same_contents(old)  # type: ignore
        upstream_macro_change = self.check_macros_modified(new)
        return different_contents or upstream_macro_change

    def check_modified_macros(self, _, new: SelectorTarget) -> bool:
        return self.check_macros_modified(new)

    @staticmethod
    def check_modified_factory(
        compare_method: str,
    ) -> Callable[[Optional[SelectorTarget], SelectorTarget], bool]:
        # get a function that compares two selector target based on compare method provided
        def check_modified_things(old: Optional[SelectorTarget], new: SelectorTarget) -> bool:
            if hasattr(new, compare_method):
                # when old body does not exist or old and new are not the same
                return not old or not getattr(new, compare_method)(old)  # type: ignore
            else:
                return False

        return check_modified_things

    def check_new(self, old: Optional[SelectorTarget], new: SelectorTarget) -> bool:
        return old is None

    def search(self, included_nodes: Set[UniqueId], selector: str) -> Iterator[UniqueId]:
        if self.previous_state is None or self.previous_state.manifest is None:
            raise DbtRuntimeError("Got a state selector method, but no comparison manifest")

        state_checks = {
            # it's new if there is no old version
            "new": lambda old, _: old is None,
            # use methods defined above to compare properties of old + new
            "modified": self.check_modified_content,
            "modified.body": self.check_modified_factory("same_body"),
            "modified.configs": self.check_modified_factory("same_config"),
            "modified.persisted_descriptions": self.check_modified_factory(
                "same_persisted_description"
            ),
            "modified.relation": self.check_modified_factory("same_database_representation"),
            "modified.macros": self.check_modified_macros,
        }
        if selector in state_checks:
            checker = state_checks[selector]
        else:
            raise DbtRuntimeError(
                f'Got an invalid selector "{selector}", expected one of ' f'"{list(state_checks)}"'
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
            elif node in manifest.metrics:
                previous_node = manifest.metrics[node]

            if checker(previous_node, real_node):
                yield node


class ResultSelectorMethod(SelectorMethod):
    def search(self, included_nodes: Set[UniqueId], selector: str) -> Iterator[UniqueId]:
        if self.previous_state is None or self.previous_state.results is None:
            raise DbtInternalError("No comparison run_results")
        matches = set(
            result.unique_id for result in self.previous_state.results if result.status == selector
        )
        for node, real_node in self.all_nodes(included_nodes):
            if node in matches:
                yield node


class SourceStatusSelectorMethod(SelectorMethod):
    def search(self, included_nodes: Set[UniqueId], selector: str) -> Iterator[UniqueId]:

        if self.previous_state is None or self.previous_state.sources is None:
            raise DbtInternalError(
                "No previous state comparison freshness results in sources.json"
            )
        elif self.previous_state.sources_current is None:
            raise DbtInternalError("No current state comparison freshness results in sources.json")

        current_state_sources = {
            result.unique_id: getattr(result, "max_loaded_at", 0)
            for result in self.previous_state.sources_current.results
            if hasattr(result, "max_loaded_at")
        }

        current_state_sources_runtime_error = {
            result.unique_id
            for result in self.previous_state.sources_current.results
            if not hasattr(result, "max_loaded_at")
        }

        previous_state_sources = {
            result.unique_id: getattr(result, "max_loaded_at", 0)
            for result in self.previous_state.sources.results
            if hasattr(result, "max_loaded_at")
        }

        previous_state_sources_runtime_error = {
            result.unique_id
            for result in self.previous_state.sources_current.results
            if not hasattr(result, "max_loaded_at")
        }

        matches = set()
        if selector == "fresher":
            for unique_id in current_state_sources:
                if unique_id not in previous_state_sources:
                    matches.add(unique_id)
                elif current_state_sources[unique_id] > previous_state_sources[unique_id]:
                    matches.add(unique_id)

            for unique_id in matches:
                if (
                    unique_id in previous_state_sources_runtime_error
                    or unique_id in current_state_sources_runtime_error
                ):
                    matches.remove(unique_id)

        for node, real_node in self.all_nodes(included_nodes):
            if node in matches:
                yield node


class MethodManager:
    SELECTOR_METHODS: Dict[MethodName, Type[SelectorMethod]] = {
        MethodName.FQN: QualifiedNameSelectorMethod,
        MethodName.Tag: TagSelectorMethod,
        MethodName.Group: GroupSelectorMethod,
        MethodName.Source: SourceSelectorMethod,
        MethodName.Path: PathSelectorMethod,
        MethodName.File: FileSelectorMethod,
        MethodName.Package: PackageSelectorMethod,
        MethodName.Config: ConfigSelectorMethod,
        MethodName.TestName: TestNameSelectorMethod,
        MethodName.TestType: TestTypeSelectorMethod,
        MethodName.ResourceType: ResourceTypeSelectorMethod,
        MethodName.State: StateSelectorMethod,
        MethodName.Exposure: ExposureSelectorMethod,
        MethodName.Metric: MetricSelectorMethod,
        MethodName.Result: ResultSelectorMethod,
        MethodName.SourceStatus: SourceStatusSelectorMethod,
    }

    def __init__(
        self,
        manifest: Manifest,
        previous_state: Optional[PreviousState],
    ):
        self.manifest = manifest
        self.previous_state = previous_state

    def get_method(self, method: MethodName, method_arguments: List[str]) -> SelectorMethod:

        if method not in self.SELECTOR_METHODS:
            raise DbtInternalError(
                f'Method name "{method}" is a valid node selection '
                f"method name, but it is not handled"
            )
        cls: Type[SelectorMethod] = self.SELECTOR_METHODS[method]
        return cls(self.manifest, self.previous_state, method_arguments)
