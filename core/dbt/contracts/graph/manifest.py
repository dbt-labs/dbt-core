import enum
import hashlib
import os
from dataclasses import dataclass, field
from datetime import datetime
from itertools import chain
from typing import (
    Dict, List, Optional, Union, Mapping, MutableMapping, Any, Set, Tuple,
    TypeVar, Callable, Iterable, Generic
)
from typing_extensions import Protocol
from uuid import UUID

from hologram import JsonSchemaMixin

from dbt.contracts.graph.parsed import (
    ParsedNode, ParsedMacro, ParsedDocumentation, ParsedNodePatch,
    ParsedMacroPatch, ParsedSourceDefinition
)
from dbt.contracts.graph.compiled import CompileResultNode, NonSourceNode
from dbt.contracts.util import Writable, Replaceable
from dbt.exceptions import (
    raise_duplicate_resource_name, InternalException, raise_compiler_error,
    warn_or_error
)
from dbt.include.global_project import PACKAGES
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType
from dbt.ui import printer
from dbt import deprecations
from dbt import tracking
import dbt.utils

NodeEdgeMap = Dict[str, List[str]]
MacroKey = Tuple[str, str]


@dataclass
class FilePath(JsonSchemaMixin):
    searched_path: str
    relative_path: str
    project_root: str

    @property
    def search_key(self) -> str:
        # TODO: should this be project name + path relative to project root?
        return self.absolute_path

    @property
    def full_path(self) -> str:
        # useful for symlink preservation
        return os.path.join(
            self.project_root, self.searched_path, self.relative_path
        )

    @property
    def absolute_path(self) -> str:
        return os.path.abspath(self.full_path)

    @property
    def original_file_path(self) -> str:
        # this is mostly used for reporting errors. It doesn't show the project
        # name, should it?
        return os.path.join(
            self.searched_path, self.relative_path
        )


@dataclass
class FileHash(JsonSchemaMixin):
    name: str  # the hash type name
    checksum: str  # the hashlib.hash_type().hexdigest() of the file contents

    @classmethod
    def empty(cls):
        return FileHash(name='none', checksum='')

    @classmethod
    def path(cls, path: str):
        return FileHash(name='path', checksum=path)

    def __eq__(self, other):
        if not isinstance(other, FileHash):
            return NotImplemented

        if self.name == 'none' or self.name != other.name:
            return False

        return self.checksum == other.checksum

    def compare(self, contents: str) -> bool:
        """Compare the file contents with the given hash"""
        if self.name == 'none':
            return False

        return self.from_contents(contents, name=self.name) == self.checksum

    @classmethod
    def from_contents(cls, contents: str, name='sha256'):
        """Create a file hash from the given file contents. The hash is always
        the utf-8 encoding of the contents given, because dbt only reads files
        as utf-8.
        """
        data = contents.encode('utf-8')
        checksum = hashlib.new(name, data).hexdigest()
        return cls(name=name, checksum=checksum)


@dataclass
class RemoteFile(JsonSchemaMixin):
    @property
    def searched_path(self) -> str:
        return 'from remote system'

    @property
    def relative_path(self) -> str:
        return 'from remote system'

    @property
    def absolute_path(self) -> str:
        return 'from remote system'

    @property
    def original_file_path(self):
        return 'from remote system'


@dataclass
class SourceFile(JsonSchemaMixin):
    """Define a source file in dbt"""
    path: Union[FilePath, RemoteFile]  # the path information
    checksum: FileHash
    # we don't want to serialize this
    _contents: Optional[str] = None
    # the unique IDs contained in this file
    nodes: List[str] = field(default_factory=list)
    docs: List[str] = field(default_factory=list)
    macros: List[str] = field(default_factory=list)
    sources: List[str] = field(default_factory=list)
    # any node patches in this file. The entries are names, not unique ids!
    patches: List[str] = field(default_factory=list)
    # any macro patches in this file. The entries are pacakge, name pairs.
    macro_patches: List[MacroKey] = field(default_factory=list)

    @property
    def search_key(self) -> Optional[str]:
        if isinstance(self.path, RemoteFile):
            return None
        if self.checksum.name == 'none':
            return None
        return self.path.search_key

    @property
    def contents(self) -> str:
        if self._contents is None:
            raise InternalException('SourceFile has no contents!')
        return self._contents

    @contents.setter
    def contents(self, value):
        self._contents = value

    @classmethod
    def empty(cls, path: FilePath) -> 'SourceFile':
        self = cls(path=path, checksum=FileHash.empty())
        self.contents = ''
        return self

    @classmethod
    def seed(cls, path: FilePath) -> 'SourceFile':
        """Seeds always parse the same regardless of their content."""
        self = cls(path=path, checksum=FileHash.path(path.absolute_path))
        self.contents = ''
        return self

    @classmethod
    def remote(cls, contents: str) -> 'SourceFile':
        self = cls(path=RemoteFile(), checksum=FileHash.empty())
        self.contents = contents
        return self


@dataclass
class ManifestMetadata(JsonSchemaMixin, Replaceable):
    project_id: Optional[str] = None
    user_id: Optional[UUID] = None
    send_anonymous_usage_stats: Optional[bool] = None
    adapter_type: Optional[str] = None

    def __post_init__(self):
        if tracking.active_user is None:
            return

        if self.user_id is None:
            self.user_id = tracking.active_user.id

        if self.send_anonymous_usage_stats is None:
            self.send_anonymous_usage_stats = (
                not tracking.active_user.do_not_track
            )


def _sort_values(dct):
    """Given a dictionary, sort each value. This makes output deterministic,
    which helps for tests.
    """
    return {k: sorted(v) for k, v in dct.items()}


def build_edges(nodes):
    """Build the forward and backward edges on the given list of ParsedNodes
    and return them as two separate dictionaries, each mapping unique IDs to
    lists of edges.
    """
    backward_edges: Dict[str, List[str]] = {}
    # pre-populate the forward edge dict for simplicity
    forward_edges: Dict[str, List[str]] = {n.unique_id: [] for n in nodes}
    for node in nodes:
        backward_edges[node.unique_id] = node.depends_on_nodes[:]
        for unique_id in node.depends_on_nodes:
            forward_edges[unique_id].append(node.unique_id)
    return _sort_values(forward_edges), _sort_values(backward_edges)


def _deepcopy(value):
    return value.from_dict(value.to_dict())


class Locality(enum.IntEnum):
    Core = 1
    Imported = 2
    Root = 3


class Specificity(enum.IntEnum):
    Default = 1
    Adapter = 2


@dataclass
class MacroCandidate:
    locality: Locality
    macro: ParsedMacro

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MacroCandidate):
            return NotImplemented
        return self.locality == other.locality

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, MacroCandidate):
            return NotImplemented
        if self.locality < other.locality:
            return True
        if self.locality > other.locality:
            return False
        return False


@dataclass
class MaterializationCandidate(MacroCandidate):
    specificity: Specificity

    @classmethod
    def from_macro(
        cls, candidate: MacroCandidate, specificity: Specificity
    ) -> 'MaterializationCandidate':
        return cls(
            locality=candidate.locality,
            macro=candidate.macro,
            specificity=specificity,
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MaterializationCandidate):
            return NotImplemented
        equal = (
            self.specificity == other.specificity and
            self.locality == other.locality
        )
        if equal:
            raise_compiler_error(
                'Found two materializations with the name {} (packages {} and '
                '{}). dbt cannot resolve this ambiguity'
                .format(self.macro.name, self.macro.package_name,
                        other.macro.package_name)
            )

        return equal

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, MaterializationCandidate):
            return NotImplemented
        if self.specificity < other.specificity:
            return True
        if self.specificity > other.specificity:
            return False
        if self.locality < other.locality:
            return True
        if self.locality > other.locality:
            return False
        return False


M = TypeVar('M', bound=MacroCandidate)


class CandidateList(List[M]):
    def last(self) -> Optional[ParsedMacro]:
        if not self:
            return None
        self.sort()
        return self[-1].macro


def _get_locality(macro: ParsedMacro, root_project_name: str) -> Locality:
    if macro.package_name == root_project_name:
        return Locality.Root
    elif macro.package_name in PACKAGES:
        return Locality.Core
    else:
        return Locality.Imported


class Searchable(Protocol):
    resource_type: NodeType
    package_name: str

    @property
    def search_name(self) -> str:
        raise NotImplementedError('search_name not implemented')


N = TypeVar('N', bound=Searchable)


@dataclass
class NameSearcher(Generic[N]):
    name: str
    package: Optional[str]
    nodetypes: List[NodeType]

    def _matches(self, model: N) -> bool:
        """Return True if the model matches the given name, package, and type.

        If package is None, any package is allowed.
        nodetypes should be a container of NodeTypes that implements the 'in'
        operator.
        """
        if model.resource_type not in self.nodetypes:
            return False

        if self.name != model.search_name:
            return False

        return self.package is None or self.package == model.package_name

    def search(self, haystack: Iterable[N]) -> Optional[N]:
        """Find an entry in the given iterable by name."""
        for model in haystack:
            if self._matches(model):
                return model
        return None


@dataclass
class Disabled:
    target: ParsedNode


@dataclass
class Manifest:
    """The manifest for the full graph, after parsing and during compilation.
    """
    nodes: MutableMapping[str, CompileResultNode]
    macros: MutableMapping[str, ParsedMacro]
    docs: MutableMapping[str, ParsedDocumentation]
    generated_at: datetime
    disabled: List[ParsedNode]
    files: MutableMapping[str, SourceFile]
    metadata: ManifestMetadata = field(default_factory=ManifestMetadata)
    flat_graph: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_macros(
        cls,
        macros: Optional[MutableMapping[str, ParsedMacro]] = None,
        files: Optional[MutableMapping[str, SourceFile]] = None,
    ) -> 'Manifest':
        if macros is None:
            macros = {}
        if files is None:
            files = {}
        return cls(
            nodes={},
            macros=macros,
            docs={},
            generated_at=datetime.utcnow(),
            disabled=[],
            files=files,
        )

    def update_node(self, new_node):
        unique_id = new_node.unique_id
        if unique_id not in self.nodes:
            raise dbt.exceptions.RuntimeException(
                'got an update_node call with an unrecognized node: {}'
                .format(unique_id)
            )
        existing = self.nodes[unique_id]
        if new_node.original_file_path != existing.original_file_path:
            raise dbt.exceptions.RuntimeException(
                'cannot update a node to have a new file path!'
            )
        self.nodes[unique_id] = new_node

    def build_flat_graph(self):
        """This attribute is used in context.common by each node, so we want to
        only build it once and avoid any concurrency issues around it.
        Make sure you don't call this until you're done with building your
        manifest!
        """
        self.flat_graph = {
            'nodes': {
                k: v.to_dict(omit_none=False) for k, v in self.nodes.items()
            },
        }

    def find_disabled_by_name(
        self, name: str, package: Optional[str] = None
    ) -> Optional[ParsedNode]:
        searcher: NameSearcher = NameSearcher(
            name, package, NodeType.refable()
        )
        result = searcher.search(self.disabled)
        if result is not None:
            assert isinstance(result, ParsedNode)
        return result

    def find_docs_by_name(
        self, name: str, package: Optional[str] = None
    ) -> Optional[ParsedDocumentation]:
        searcher: NameSearcher = NameSearcher(
            name, package, [NodeType.Documentation]
        )
        result = searcher.search(self.docs.values())
        if result is not None:
            assert isinstance(result, ParsedDocumentation)
        return result

    def find_refable_by_name(
        self, name: str, package: Optional[str]
    ) -> Optional[NonSourceNode]:
        """Find any valid target for "ref()" in the graph by its name and
        package name, or None for any package.
        """
        searcher: NameSearcher = NameSearcher(
            name, package, NodeType.refable()
        )
        result = searcher.search(self.nodes.values())
        if result is not None:
            assert not isinstance(result, ParsedSourceDefinition)
        return result

    def find_source_by_name(
        self, source_name: str, table_name: str, package: Optional[str]
    ) -> Optional[ParsedSourceDefinition]:
        """Find any valid target for "source()" in the graph by its name and
        package name, or None for any package.
        """

        name = f'{source_name}.{table_name}'
        searcher: NameSearcher = NameSearcher(name, package, [NodeType.Source])
        result = searcher.search(self.nodes.values())
        if result is not None:
            assert isinstance(result, ParsedSourceDefinition)
        return result

    def _find_macros_by_name(
        self,
        name: str,
        root_project_name: str,
        filter: Optional[Callable[[MacroCandidate], bool]] = None
    ) -> CandidateList:
        """Find macros by their name.
        """
        candidates: CandidateList = CandidateList()
        for unique_id, macro in self.macros.items():
            if macro.name != name:
                continue
            candidate = MacroCandidate(
                locality=_get_locality(macro, root_project_name),
                macro=macro,
            )
            if filter is None or filter(candidate):
                candidates.append(candidate)

        return candidates

    def _materialization_candidates_for(
        self, project_name: str,
        materialization_name: str,
        adapter_type: Optional[str],
    ) -> CandidateList:

        if adapter_type is None:
            specificity = Specificity.Default
        else:
            specificity = Specificity.Adapter

        full_name = dbt.utils.get_materialization_macro_name(
            materialization_name=materialization_name,
            adapter_type=adapter_type,
            with_prefix=False,
        )
        return CandidateList(
            MaterializationCandidate.from_macro(m, specificity)
            for m in self._find_macros_by_name(full_name, project_name)
        )

    def find_macro_by_name(
        self, name: str, root_project_name: str, package: Optional[str]
    ) -> Optional[ParsedMacro]:
        """Find a macro in the graph by its name and package name, or None for
        any package. The root project name is used to determine priority:
         - locally defined macros come first
         - then imported macros
         - then macros defined in the root project
        """
        filter: Optional[Callable[[MacroCandidate], bool]] = None
        if package is not None:
            def filter(candidate: MacroCandidate) -> bool:
                return package == candidate.macro.package_name

        candidates: CandidateList = self._find_macros_by_name(
            name=name,
            root_project_name=root_project_name,
            filter=filter,
        )

        return candidates.last()

    def find_generate_macro_by_name(
        self, component: str, root_project_name: str
    ) -> Optional[ParsedMacro]:
        """
        The `generate_X_name` macros are similar to regular ones, but ignore
        imported packages.
            - if there is a `generate_{component}_name` macro in the root
              project, return it
            - return the `generate_{component}_name` macro from the 'dbt'
              internal project
        """
        def filter(candidate: MacroCandidate) -> bool:
            return candidate.locality != Locality.Imported

        candidates: CandidateList = self._find_macros_by_name(
            name=f'generate_{component}_name',
            root_project_name=root_project_name,
            # filter out imported packages
            filter=filter,
        )
        return candidates.last()

    def find_materialization_macro_by_name(
        self, project_name: str, materialization_name: str, adapter_type: str
    ) -> Optional[ParsedMacro]:
        candidates: CandidateList = CandidateList(chain.from_iterable(
            self._materialization_candidates_for(
                project_name=project_name,
                materialization_name=materialization_name,
                adapter_type=atype,
            ) for atype in (adapter_type, None)
        ))
        return candidates.last()

    def get_resource_fqns(self) -> Dict[str, Set[Tuple[str, ...]]]:
        resource_fqns: Dict[str, Set[Tuple[str, ...]]] = {}
        for unique_id, node in self.nodes.items():
            if node.resource_type == NodeType.Source:
                continue  # sources have no FQNs and can't be configured
            resource_type_plural = node.resource_type + 's'
            if resource_type_plural not in resource_fqns:
                resource_fqns[resource_type_plural] = set()
            resource_fqns[resource_type_plural].add(tuple(node.fqn))

        return resource_fqns

    def add_nodes(self, new_nodes):
        """Add the given dict of new nodes to the manifest."""
        for unique_id, node in new_nodes.items():
            if unique_id in self.nodes:
                raise_duplicate_resource_name(node, self.nodes[unique_id])
            self.nodes[unique_id] = node

    def patch_macros(
        self, patches: MutableMapping[MacroKey, ParsedMacroPatch]
    ) -> None:
        for macro in self.macros.values():
            key = (macro.package_name, macro.name)
            patch = patches.pop(key, None)
            if not patch:
                continue
            macro.patch(patch)

        if patches:
            for patch in patches.values():
                warn_or_error(
                    f'WARNING: Found documentation for macro "{patch.name}" '
                    f'which was not found'
                )

    def patch_nodes(
        self, patches: MutableMapping[str, ParsedNodePatch]
    ) -> None:
        """Patch nodes with the given dict of patches. Note that this consumes
        the input!
        This relies on the fact that all nodes have unique _name_ fields, not
        just unique unique_id fields.
        """
        # because we don't have any mapping from node _names_ to nodes, and we
        # only have the node name in the patch, we have to iterate over all the
        # nodes looking for matching names. We could use a NameSearcher if we
        # were ok with doing an O(n*m) search (one nodes scan per patch)
        for node in self.nodes.values():
            if node.resource_type == NodeType.Source:
                continue
            # appease mypy - we know this because of the check above
            assert not isinstance(node, ParsedSourceDefinition)
            patch = patches.pop(node.name, None)
            if not patch:
                continue

            expected_key = node.resource_type.pluralize()
            if expected_key != patch.yaml_key:
                if patch.yaml_key == 'models':
                    deprecations.warn(
                        'models-key-mismatch',
                        patch=patch, node=node, expected_key=expected_key
                    )
                else:
                    msg = printer.line_wrap_message(
                        f'''\
                        '{node.name}' is a {node.resource_type} node, but it is
                        specified in the {patch.yaml_key} section of
                        {patch.original_file_path}.



                        To fix this error, place the `{node.name}`
                        specification under the {expected_key} key instead.
                        '''
                    )
                    raise_compiler_error(msg)

            node.patch(patch)

        # log debug-level warning about nodes we couldn't find
        if patches:
            for patch in patches.values():
                # since patches aren't nodes, we can't use the existing
                # target_not_found warning
                logger.debug((
                    'WARNING: Found documentation for resource "{}" which was '
                    'not found or is disabled').format(patch.name)
                )

    def get_used_schemas(self, resource_types=None):
        return frozenset({
            (node.database, node.schema)
            for node in self.nodes.values()
            if not resource_types or node.resource_type in resource_types
        })

    def get_used_databases(self):
        return frozenset(node.database for node in self.nodes.values())

    def deepcopy(self):
        return Manifest(
            nodes={k: _deepcopy(v) for k, v in self.nodes.items()},
            macros={k: _deepcopy(v) for k, v in self.macros.items()},
            docs={k: _deepcopy(v) for k, v in self.docs.items()},
            generated_at=self.generated_at,
            disabled=[_deepcopy(n) for n in self.disabled],
            metadata=self.metadata,
            files={k: _deepcopy(v) for k, v in self.files.items()},
        )

    def writable_manifest(self):
        forward_edges, backward_edges = build_edges(self.nodes.values())

        return WritableManifest(
            nodes=self.nodes,
            macros=self.macros,
            docs=self.docs,
            generated_at=self.generated_at,
            metadata=self.metadata,
            disabled=self.disabled,
            child_map=forward_edges,
            parent_map=backward_edges,
            files=self.files,
        )

    @classmethod
    def from_writable_manifest(cls, writable):
        self = cls(
            nodes=writable.nodes,
            macros=writable.macros,
            docs=writable.docs,
            generated_at=writable.generated_at,
            metadata=writable.metadata,
            disabled=writable.disabled,
            files=writable.files,
        )
        self.metadata = writable.metadata
        return self

    @classmethod
    def from_dict(cls, data, validate=True):
        writable = WritableManifest.from_dict(data=data, validate=validate)
        return cls.from_writable_manifest(writable)

    def to_dict(self, omit_none=True, validate=False):
        return self.writable_manifest().to_dict(
            omit_none=omit_none, validate=validate
        )

    def write(self, path):
        self.writable_manifest().write(path)

    def expect(self, unique_id: str) -> CompileResultNode:
        if unique_id not in self.nodes:
            # something terrible has happened
            raise dbt.exceptions.InternalException(
                'Expected node {} not found in manifest'.format(unique_id)
            )
        return self.nodes[unique_id]

    def resolve_ref(
        self,
        target_model_name: str,
        target_model_package: Optional[str],
        current_project: str,
        node_package: str,
    ) -> Optional[Union[NonSourceNode, Disabled]]:
        if target_model_package is not None:
            return self.find_refable_by_name(
                target_model_name,
                target_model_package)

        target_model = None
        disabled_target = None

        # first pass: look for models in the current_project
        # second pass: look for models in the node's package
        # final pass: look for models in any package
        if current_project == node_package:
            candidates = [current_project, None]
        else:
            candidates = [current_project, node_package, None]
        for candidate in candidates:
            target_model = self.find_refable_by_name(
                target_model_name,
                candidate)

            if target_model is not None and target_model.config.enabled:
                return target_model

            # it's possible that the node is disabled
            if disabled_target is None:
                disabled_target = self.find_disabled_by_name(
                    target_model_name, candidate
                )

        if disabled_target is not None:
            return Disabled(disabled_target)
        return None

    def resolve_source(
        self,
        target_source_name: str,
        target_table_name: str,
        current_project: str,
        node_package: str
    ) -> Optional[ParsedSourceDefinition]:
        candidate_targets = [current_project, node_package, None]
        target_source = None
        for candidate in candidate_targets:
            target_source = self.find_source_by_name(
                target_source_name,
                target_table_name,
                candidate
            )
            if target_source is not None:
                return target_source

        return None

    def resolve_doc(
        self,
        name: str,
        package: Optional[str],
        current_project: str,
        node_package: str,
    ) -> Optional[ParsedDocumentation]:
        """Resolve the given documentation. This follows the same algorithm as
        resolve_ref except the is_enabled checks are unnecessary as docs are
        always enabled.
        """
        if package is not None:
            return self.find_docs_by_name(
                name, package
            )

        candidate_targets = [
            current_project,
            node_package,
            None,
        ]
        target_doc = None
        for candidate in candidate_targets:
            target_doc = self.find_docs_by_name(name, candidate)
            if target_doc is not None:
                break
        return target_doc


@dataclass
class WritableManifest(JsonSchemaMixin, Writable):
    nodes: Mapping[str, CompileResultNode]
    macros: Mapping[str, ParsedMacro]
    docs: Mapping[str, ParsedDocumentation]
    disabled: Optional[List[ParsedNode]]
    generated_at: datetime
    parent_map: Optional[NodeEdgeMap]
    child_map: Optional[NodeEdgeMap]
    metadata: ManifestMetadata
    # map of original_file_path to all unique IDs provided by that file
    files: Mapping[str, SourceFile]
