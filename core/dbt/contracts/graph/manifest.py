import enum
from dataclasses import dataclass, field
from itertools import chain, islice
from mashumaro import DataClassMessagePackMixin
from multiprocessing.synchronize import Lock
from typing import (
    Dict, List, Optional, Union, Mapping, MutableMapping, Any, Set, Tuple,
    TypeVar, Callable, Generic, cast, AbstractSet, ClassVar
)
from typing_extensions import Protocol
from uuid import UUID

from dbt.contracts.graph.compiled import (
    CompileResultNode, ManifestNode, NonSourceCompiledNode, GraphMemberNode
)
from dbt.contracts.graph.parsed import (
    ParsedMacro, ParsedDocumentation,
    ParsedSourceDefinition, ParsedExposure, HasUniqueID,
    UnpatchedSourceDefinition, ManifestNodes
)
from dbt.contracts.graph.unparsed import SourcePatch
from dbt.contracts.files import SourceFile, SchemaSourceFile, FileHash, AnySourceFile
from dbt.contracts.util import (
    BaseArtifactMetadata, SourceKey, ArtifactMixin, schema_version
)
from dbt.dataclass_schema import dbtClassMixin
from dbt.exceptions import (
    CompilationException,
    raise_duplicate_resource_name, raise_compiler_error,
)
from dbt.helper_types import PathSet
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType
from dbt.ui import line_wrap_message
from dbt import flags
from dbt import tracking
import dbt.utils

NodeEdgeMap = Dict[str, List[str]]
PackageName = str
DocName = str
RefName = str
UniqueID = str


def find_unique_id_for_package(storage, key, package: Optional[PackageName]):
    if key not in storage:
        return None

    pkg_dct: Mapping[PackageName, UniqueID] = storage[key]

    if package is None:
        if not pkg_dct:
            return None
        else:
            return next(iter(pkg_dct.values()))
    elif package in pkg_dct:
        return pkg_dct[package]
    else:
        return None


class DocLookup(dbtClassMixin):
    def __init__(self, manifest: 'Manifest'):
        self.storage: Dict[str, Dict[PackageName, UniqueID]] = {}
        self.populate(manifest)

    def get_unique_id(self, key, package: Optional[PackageName]):
        return find_unique_id_for_package(self.storage, key, package)

    def find(self, key, package: Optional[PackageName], manifest: 'Manifest'):
        unique_id = self.get_unique_id(key, package)
        if unique_id is not None:
            return self.perform_lookup(unique_id, manifest)
        return None

    def add_doc(self, doc: ParsedDocumentation):
        if doc.name not in self.storage:
            self.storage[doc.name] = {}
        self.storage[doc.name][doc.package_name] = doc.unique_id

    def populate(self, manifest):
        for doc in manifest.docs.values():
            self.add_doc(doc)

    def perform_lookup(
        self, unique_id: UniqueID, manifest
    ) -> ParsedDocumentation:
        if unique_id not in manifest.docs:
            raise dbt.exceptions.InternalException(
                f'Doc {unique_id} found in cache but not found in manifest'
            )
        return manifest.docs[unique_id]


class SourceLookup(dbtClassMixin):
    def __init__(self, manifest: 'Manifest'):
        self.storage: Dict[str, Dict[PackageName, UniqueID]] = {}
        self.populate(manifest)

    def get_unique_id(self, search_name, package: Optional[PackageName]):
        return find_unique_id_for_package(self.storage, search_name, package)

    def find(self, search_name, package: Optional[PackageName], manifest: 'Manifest'):
        unique_id = self.get_unique_id(search_name, package)
        if unique_id is not None:
            return self.perform_lookup(unique_id, manifest)
        return None

    def add_source(self, source: ParsedSourceDefinition):
        if source.search_name not in self.storage:
            self.storage[source.search_name] = {}

        self.storage[source.search_name][source.package_name] = source.unique_id

    def populate(self, manifest):
        for source in manifest.sources.values():
            if hasattr(source, 'source_name'):
                self.add_source(source)

    def perform_lookup(
        self, unique_id: UniqueID, manifest: 'Manifest'
    ) -> ParsedSourceDefinition:
        if unique_id not in manifest.sources:
            raise dbt.exceptions.InternalException(
                f'Source {unique_id} found in cache but not found in manifest'
            )
        return manifest.sources[unique_id]


class RefableLookup(dbtClassMixin):
    # model, seed, snapshot
    _lookup_types: ClassVar[set] = set(NodeType.refable())

    # refables are actually unique, so the Dict[PackageName, UniqueID] will
    # only ever have exactly one value, but doing 3 dict lookups instead of 1
    # is not a big deal at all and retains consistency
    def __init__(self, manifest: 'Manifest'):
        self.storage: Dict[str, Dict[PackageName, UniqueID]] = {}
        self.populate(manifest)

    def get_unique_id(self, key, package: Optional[PackageName]):
        return find_unique_id_for_package(self.storage, key, package)

    def find(self, key, package: Optional[PackageName], manifest: 'Manifest'):
        unique_id = self.get_unique_id(key, package)
        if unique_id is not None:
            return self.perform_lookup(unique_id, manifest)
        return None

    def add_node(self, node: ManifestNode):
        if node.resource_type in self._lookup_types:
            if node.name not in self.storage:
                self.storage[node.name] = {}
            self.storage[node.name][node.package_name] = node.unique_id

    def populate(self, manifest):
        for node in manifest.nodes.values():
            self.add_node(node)

    def perform_lookup(
        self, unique_id: UniqueID, manifest
    ) -> ManifestNode:
        if unique_id not in manifest.nodes:
            raise dbt.exceptions.InternalException(
                f'Node {unique_id} found in cache but not found in manifest'
            )
        return manifest.nodes[unique_id]


# This handles both models/seeds/snapshots and sources
class DisabledLookup(dbtClassMixin):

    def __init__(self, manifest: 'Manifest'):
        self.storage: Dict[str, Dict[PackageName, List[Any]]] = {}
        self.populate(manifest)

    def populate(self, manifest):
        for node in list(chain.from_iterable(manifest.disabled.values())):
            self.add_node(node)

    def add_node(self, node):
        if node.search_name not in self.storage:
            self.storage[node.search_name] = {}
        if node.package_name not in self.storage[node.search_name]:
            self.storage[node.search_name][node.package_name] = []
        self.storage[node.search_name][node.package_name].append(node)

    # This should return a list of disabled nodes. It's different from
    # the other Lookup functions in that it returns full nodes, not just unique_ids
    def find(self, search_name, package: Optional[PackageName]):
        if search_name not in self.storage:
            return None

        pkg_dct: Mapping[PackageName, List[Any]] = self.storage[search_name]

        if package is None:
            if not pkg_dct:
                return None
            else:
                return next(iter(pkg_dct.values()))
        elif package in pkg_dct:
            return pkg_dct[package]
        else:
            return None


class AnalysisLookup(RefableLookup):
    _lookup_types: ClassVar[set] = set([NodeType.Analysis])


def _search_packages(
    current_project: str,
    node_package: str,
    target_package: Optional[str] = None,
) -> List[Optional[str]]:
    if target_package is not None:
        return [target_package]
    elif current_project == node_package:
        return [current_project, None]
    else:
        return [current_project, node_package, None]


@dataclass
class ManifestMetadata(BaseArtifactMetadata):
    """Metadata for the manifest."""
    dbt_schema_version: str = field(
        default_factory=lambda: str(WritableManifest.dbt_schema_version)
    )
    project_id: Optional[str] = field(
        default=None,
        metadata={
            'description': 'A unique identifier for the project',
        },
    )
    user_id: Optional[UUID] = field(
        default=None,
        metadata={
            'description': 'A unique identifier for the user',
        },
    )
    send_anonymous_usage_stats: Optional[bool] = field(
        default=None,
        metadata=dict(description=(
            'Whether dbt is configured to send anonymous usage statistics'
        )),
    )
    adapter_type: Optional[str] = field(
        default=None,
        metadata=dict(description='The type name of the adapter'),
    )

    def __post_init__(self):
        if tracking.active_user is None:
            return

        if self.user_id is None:
            self.user_id = tracking.active_user.id

        if self.send_anonymous_usage_stats is None:
            self.send_anonymous_usage_stats = flags.SEND_ANONYMOUS_USAGE_STATS

    @classmethod
    def default(cls):
        return cls(
            dbt_schema_version=str(WritableManifest.dbt_schema_version),
        )


def _sort_values(dct):
    """Given a dictionary, sort each value. This makes output deterministic,
    which helps for tests.
    """
    return {k: sorted(v) for k, v in dct.items()}


def build_node_edges(nodes: List[ManifestNode]):
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
            if unique_id in forward_edges.keys():
                forward_edges[unique_id].append(node.unique_id)
    return _sort_values(forward_edges), _sort_values(backward_edges)


# Build a map of children of macros and generic tests
def build_macro_edges(nodes: List[Any]):
    forward_edges: Dict[str, List[str]] = {
        n.unique_id: [] for n in nodes if n.unique_id.startswith('macro') or n.depends_on.macros
    }
    for node in nodes:
        for unique_id in node.depends_on.macros:
            if unique_id in forward_edges.keys():
                forward_edges[unique_id].append(node.unique_id)
    return _sort_values(forward_edges)


def _deepcopy(value):
    return value.from_dict(value.to_dict(omit_none=True))


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


def _get_locality(
    macro: ParsedMacro, root_project_name: str, internal_packages: Set[str]
) -> Locality:
    if macro.package_name == root_project_name:
        return Locality.Root
    elif macro.package_name in internal_packages:
        return Locality.Core
    else:
        return Locality.Imported


class Searchable(Protocol):
    resource_type: NodeType
    package_name: str

    @property
    def search_name(self) -> str:
        raise NotImplementedError('search_name not implemented')


D = TypeVar('D')


@dataclass
class Disabled(Generic[D]):
    target: D


MaybeDocumentation = Optional[ParsedDocumentation]


MaybeParsedSource = Optional[Union[
    ParsedSourceDefinition,
    Disabled[ParsedSourceDefinition],
]]


MaybeNonSource = Optional[Union[
    ManifestNode,
    Disabled[ManifestNode]
]]


T = TypeVar('T', bound=GraphMemberNode)


def _update_into(dest: MutableMapping[str, T], new_item: T):
    """Update dest to overwrite whatever is at dest[new_item.unique_id] with
    new_itme. There must be an existing value to overwrite, and they two nodes
    must have the same original file path.
    """
    unique_id = new_item.unique_id
    if unique_id not in dest:
        raise dbt.exceptions.RuntimeException(
            f'got an update_{new_item.resource_type} call with an '
            f'unrecognized {new_item.resource_type}: {new_item.unique_id}'
        )
    existing = dest[unique_id]
    if new_item.original_file_path != existing.original_file_path:
        raise dbt.exceptions.RuntimeException(
            f'cannot update a {new_item.resource_type} to have a new file '
            f'path!'
        )
    dest[unique_id] = new_item


# This contains macro methods that are in both the Manifest
# and the MacroManifest
class MacroMethods:
    # Just to make mypy happy. There must be a better way.
    def __init__(self):
        self.macros = []
        self.metadata = {}

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

    def _find_macros_by_name(
        self,
        name: str,
        root_project_name: str,
        filter: Optional[Callable[[MacroCandidate], bool]] = None
    ) -> CandidateList:
        """Find macros by their name.
        """
        # avoid an import cycle
        from dbt.adapters.factory import get_adapter_package_names
        candidates: CandidateList = CandidateList()
        packages = set(get_adapter_package_names(self.metadata.adapter_type))
        for unique_id, macro in self.macros.items():
            if macro.name != name:
                continue
            candidate = MacroCandidate(
                locality=_get_locality(macro, root_project_name, packages),
                macro=macro,
            )
            if filter is None or filter(candidate):
                candidates.append(candidate)

        return candidates


@dataclass
class ParsingInfo:
    static_analysis_parsed_path_count: int = 0
    static_analysis_path_count: int = 0


@dataclass
class ManifestStateCheck(dbtClassMixin):
    vars_hash: FileHash = field(default_factory=FileHash.empty)
    profile_hash: FileHash = field(default_factory=FileHash.empty)
    project_hashes: MutableMapping[str, FileHash] = field(default_factory=dict)


@dataclass
class Manifest(MacroMethods, DataClassMessagePackMixin, dbtClassMixin):
    """The manifest for the full graph, after parsing and during compilation.
    """
    # These attributes are both positional and by keyword. If an attribute
    # is added it must all be added in the __reduce_ex__ method in the
    # args tuple in the right position.
    nodes: MutableMapping[str, ManifestNode] = field(default_factory=dict)
    sources: MutableMapping[str, ParsedSourceDefinition] = field(default_factory=dict)
    macros: MutableMapping[str, ParsedMacro] = field(default_factory=dict)
    docs: MutableMapping[str, ParsedDocumentation] = field(default_factory=dict)
    exposures: MutableMapping[str, ParsedExposure] = field(default_factory=dict)
    selectors: MutableMapping[str, Any] = field(default_factory=dict)
    files: MutableMapping[str, AnySourceFile] = field(default_factory=dict)
    metadata: ManifestMetadata = field(default_factory=ManifestMetadata)
    flat_graph: Dict[str, Any] = field(default_factory=dict)
    state_check: ManifestStateCheck = field(default_factory=ManifestStateCheck)
    source_patches: MutableMapping[SourceKey, SourcePatch] = field(default_factory=dict)
    disabled: MutableMapping[str, List[CompileResultNode]] = field(default_factory=dict)
    env_vars: MutableMapping[str, str] = field(default_factory=dict)

    _doc_lookup: Optional[DocLookup] = field(
        default=None, metadata={'serialize': lambda x: None, 'deserialize': lambda x: None}
    )
    _source_lookup: Optional[SourceLookup] = field(
        default=None, metadata={'serialize': lambda x: None, 'deserialize': lambda x: None}
    )
    _ref_lookup: Optional[RefableLookup] = field(
        default=None, metadata={'serialize': lambda x: None, 'deserialize': lambda x: None}
    )
    _disabled_lookup: Optional[DisabledLookup] = field(
        default=None, metadata={'serialize': lambda x: None, 'deserialize': lambda x: None}
    )
    _analysis_lookup: Optional[AnalysisLookup] = field(
        default=None, metadata={'serialize': lambda x: None, 'deserialize': lambda x: None}
    )
    _parsing_info: ParsingInfo = field(
        default_factory=ParsingInfo,
        metadata={'serialize': lambda x: None, 'deserialize': lambda x: None}
    )
    _lock: Lock = field(
        default_factory=flags.MP_CONTEXT.Lock,
        metadata={'serialize': lambda x: None, 'deserialize': lambda x: None}
    )

    def __pre_serialize__(self):
        # serialization won't work with anything except an empty source_patches because
        # tuple keys are not supported, so ensure it's empty
        self.source_patches = {}
        return self

    @classmethod
    def __post_deserialize__(cls, obj):
        obj._lock = flags.MP_CONTEXT.Lock()
        return obj

    def sync_update_node(
        self, new_node: NonSourceCompiledNode
    ) -> NonSourceCompiledNode:
        """update the node with a lock. The only time we should want to lock is
        when compiling an ephemeral ancestor of a node at runtime, because
        multiple threads could be just-in-time compiling the same ephemeral
        dependency, and we want them to have a consistent view of the manifest.

        If the existing node is not compiled, update it with the new node and
        return that. If the existing node is compiled, do not update the
        manifest and return the existing node.
        """
        with self._lock:
            existing = self.nodes[new_node.unique_id]
            if getattr(existing, 'compiled', False):
                # already compiled -> must be a NonSourceCompiledNode
                return cast(NonSourceCompiledNode, existing)
            _update_into(self.nodes, new_node)
            return new_node

    def update_exposure(self, new_exposure: ParsedExposure):
        _update_into(self.exposures, new_exposure)

    def update_node(self, new_node: ManifestNode):
        _update_into(self.nodes, new_node)

    def update_source(self, new_source: ParsedSourceDefinition):
        _update_into(self.sources, new_source)

    def build_flat_graph(self):
        """This attribute is used in context.common by each node, so we want to
        only build it once and avoid any concurrency issues around it.
        Make sure you don't call this until you're done with building your
        manifest!
        """
        self.flat_graph = {
            'exposures': {
                k: v.to_dict(omit_none=False)
                for k, v in self.exposures.items()
            },
            'nodes': {
                k: v.to_dict(omit_none=False)
                for k, v in self.nodes.items()
            },
            'sources': {
                k: v.to_dict(omit_none=False)
                for k, v in self.sources.items()
            }
        }

    def build_disabled_by_file_id(self):
        disabled_by_file_id = {}
        for node_list in self.disabled.values():
            for node in node_list:
                disabled_by_file_id[node.file_id] = node
        return disabled_by_file_id

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

    def get_resource_fqns(self) -> Mapping[str, PathSet]:
        resource_fqns: Dict[str, Set[Tuple[str, ...]]] = {}
        all_resources = chain(self.exposures.values(), self.nodes.values(), self.sources.values())
        for resource in all_resources:
            resource_type_plural = resource.resource_type.pluralize()
            if resource_type_plural not in resource_fqns:
                resource_fqns[resource_type_plural] = set()
            resource_fqns[resource_type_plural].add(tuple(resource.fqn))
        return resource_fqns

    def get_used_schemas(self, resource_types=None):
        return frozenset({
            (node.database, node.schema) for node in
            chain(self.nodes.values(), self.sources.values())
            if not resource_types or node.resource_type in resource_types
        })

    def get_used_databases(self):
        return frozenset(
            x.database for x in
            chain(self.nodes.values(), self.sources.values())
        )

    def deepcopy(self):
        return Manifest(
            nodes={k: _deepcopy(v) for k, v in self.nodes.items()},
            sources={k: _deepcopy(v) for k, v in self.sources.items()},
            macros={k: _deepcopy(v) for k, v in self.macros.items()},
            docs={k: _deepcopy(v) for k, v in self.docs.items()},
            exposures={k: _deepcopy(v) for k, v in self.exposures.items()},
            selectors={k: _deepcopy(v) for k, v in self.selectors.items()},
            metadata=self.metadata,
            disabled={k: _deepcopy(v) for k, v in self.disabled.items()},
            files={k: _deepcopy(v) for k, v in self.files.items()},
            state_check=_deepcopy(self.state_check),
        )

    def build_parent_and_child_maps(self):
        edge_members = list(chain(
            self.nodes.values(),
            self.sources.values(),
            self.exposures.values(),
        ))
        forward_edges, backward_edges = build_node_edges(edge_members)
        self.child_map = forward_edges
        self.parent_map = backward_edges

    def build_macro_child_map(self):
        edge_members = list(chain(
            self.nodes.values(),
            self.macros.values(),
        ))
        forward_edges = build_macro_edges(edge_members)
        return forward_edges

    def writable_manifest(self):
        self.build_parent_and_child_maps()
        return WritableManifest(
            nodes=self.nodes,
            sources=self.sources,
            macros=self.macros,
            docs=self.docs,
            exposures=self.exposures,
            selectors=self.selectors,
            metadata=self.metadata,
            disabled=self.disabled,
            child_map=self.child_map,
            parent_map=self.parent_map,
        )

    def write(self, path):
        self.writable_manifest().write(path)

    # Called in dbt.compilation.Linker.write_graph and
    # dbt.graph.queue.get and ._include_in_cost
    def expect(self, unique_id: str) -> GraphMemberNode:
        if unique_id in self.nodes:
            return self.nodes[unique_id]
        elif unique_id in self.sources:
            return self.sources[unique_id]
        elif unique_id in self.exposures:
            return self.exposures[unique_id]
        else:
            # something terrible has happened
            raise dbt.exceptions.InternalException(
                'Expected node {} not found in manifest'.format(unique_id)
            )

    @property
    def doc_lookup(self) -> DocLookup:
        if self._doc_lookup is None:
            self._doc_lookup = DocLookup(self)
        return self._doc_lookup

    def rebuild_doc_lookup(self):
        self._doc_lookup = DocLookup(self)

    @property
    def source_lookup(self) -> SourceLookup:
        if self._source_lookup is None:
            self._source_lookup = SourceLookup(self)
        return self._source_lookup

    def rebuild_source_lookup(self):
        self._source_lookup = SourceLookup(self)

    @property
    def ref_lookup(self) -> RefableLookup:
        if self._ref_lookup is None:
            self._ref_lookup = RefableLookup(self)
        return self._ref_lookup

    def rebuild_ref_lookup(self):
        self._ref_lookup = RefableLookup(self)

    @property
    def disabled_lookup(self) -> DisabledLookup:
        if self._disabled_lookup is None:
            self._disabled_lookup = DisabledLookup(self)
        return self._disabled_lookup

    def rebuild_disabled_lookup(self):
        self._disabled_lookup = DisabledLookup(self)

    @property
    def analysis_lookup(self) -> AnalysisLookup:
        if self._analysis_lookup is None:
            self._analysis_lookup = AnalysisLookup(self)
        return self._analysis_lookup

    # Called by dbt.parser.manifest._resolve_refs_for_exposure
    # and dbt.parser.manifest._process_refs_for_node
    def resolve_ref(
        self,
        target_model_name: str,
        target_model_package: Optional[str],
        current_project: str,
        node_package: str,
    ) -> MaybeNonSource:

        node: Optional[ManifestNode] = None
        disabled: Optional[List[ManifestNode]] = None

        candidates = _search_packages(
            current_project, node_package, target_model_package
        )
        for pkg in candidates:
            node = self.ref_lookup.find(target_model_name, pkg, self)

            if node is not None and node.config.enabled:
                return node

            # it's possible that the node is disabled
            if disabled is None:
                disabled = self.disabled_lookup.find(
                    target_model_name, pkg
                )

        if disabled:
            return Disabled(disabled[0])
        return None

    # Called by dbt.parser.manifest._resolve_sources_for_exposure
    # and dbt.parser.manifest._process_source_for_node
    def resolve_source(
        self,
        target_source_name: str,
        target_table_name: str,
        current_project: str,
        node_package: str
    ) -> MaybeParsedSource:
        search_name = f'{target_source_name}.{target_table_name}'
        candidates = _search_packages(current_project, node_package)

        source: Optional[ParsedSourceDefinition] = None
        disabled: Optional[List[ParsedSourceDefinition]] = None

        for pkg in candidates:
            source = self.source_lookup.find(search_name, pkg, self)
            if source is not None and source.config.enabled:
                return source

            if disabled is None:
                disabled = self.disabled_lookup.find(
                    f'{target_source_name}.{target_table_name}', pkg
                )

        if disabled:
            return Disabled(disabled[0])
        return None

    # Called by DocsRuntimeContext.doc
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
        candidates = _search_packages(
            current_project, node_package, package
        )

        for pkg in candidates:
            result = self.doc_lookup.find(name, pkg, self)
            if result is not None:
                return result
        return None

    # Called by RunTask.defer_to_manifest
    def merge_from_artifact(
        self,
        adapter,
        other: 'WritableManifest',
        selected: AbstractSet[UniqueID],
    ) -> None:
        """Given the selected unique IDs and a writable manifest, update this
        manifest by replacing any unselected nodes with their counterpart.

        Only non-ephemeral refable nodes are examined.
        """
        refables = set(NodeType.refable())
        merged = set()
        for unique_id, node in other.nodes.items():
            current = self.nodes.get(unique_id)
            if current and (
                node.resource_type in refables and
                not node.is_ephemeral and
                unique_id not in selected and
                not adapter.get_relation(
                    current.database, current.schema, current.identifier
                )
            ):
                merged.add(unique_id)
                self.nodes[unique_id] = node.replace(deferred=True)

        # log up to 5 items
        sample = list(islice(merged, 5))
        logger.debug(
            f'Merged {len(merged)} items from state (sample: {sample})'
        )

    # Methods that were formerly in ParseResult

    def add_macro(self, source_file: SourceFile, macro: ParsedMacro):
        if macro.unique_id in self.macros:
            # detect that the macro exists and emit an error
            other_path = self.macros[macro.unique_id].original_file_path
            # subtract 2 for the "Compilation Error" indent
            # note that the line wrap eats newlines, so if you want newlines,
            # this is the result :(
            msg = line_wrap_message(
                f'''\
                dbt found two macros named "{macro.name}" in the project
                "{macro.package_name}".


                To fix this error, rename or remove one of the following
                macros:

                    - {macro.original_file_path}

                    - {other_path}
                ''',
                subtract=2
            )
            raise_compiler_error(msg)

        self.macros[macro.unique_id] = macro
        source_file.macros.append(macro.unique_id)

    def has_file(self, source_file: SourceFile) -> bool:
        key = source_file.file_id
        if key is None:
            return False
        if key not in self.files:
            return False
        my_checksum = self.files[key].checksum
        return my_checksum == source_file.checksum

    def add_source(
        self, source_file: SchemaSourceFile, source: UnpatchedSourceDefinition
    ):
        # sources can't be overwritten!
        _check_duplicates(source, self.sources)
        self.sources[source.unique_id] = source  # type: ignore
        source_file.sources.append(source.unique_id)

    def add_node_nofile(self, node: ManifestNodes):
        # nodes can't be overwritten!
        _check_duplicates(node, self.nodes)
        self.nodes[node.unique_id] = node

    def add_node(self, source_file: AnySourceFile, node: ManifestNodes, test_from=None):
        self.add_node_nofile(node)
        if isinstance(source_file, SchemaSourceFile):
            assert test_from
            source_file.add_test(node.unique_id, test_from)
        else:
            source_file.nodes.append(node.unique_id)

    def add_exposure(self, source_file: SchemaSourceFile, exposure: ParsedExposure):
        _check_duplicates(exposure, self.exposures)
        self.exposures[exposure.unique_id] = exposure
        source_file.exposures.append(exposure.unique_id)

    def add_disabled_nofile(self, node: CompileResultNode):
        # There can be multiple disabled nodes for the same unique_id
        if node.unique_id in self.disabled:
            self.disabled[node.unique_id].append(node)
        else:
            self.disabled[node.unique_id] = [node]

    def add_disabled(self, source_file: AnySourceFile, node: CompileResultNode, test_from=None):
        self.add_disabled_nofile(node)
        if isinstance(source_file, SchemaSourceFile):
            assert test_from
            source_file.add_test(node.unique_id, test_from)
        else:
            source_file.nodes.append(node.unique_id)

    def add_doc(self, source_file: SourceFile, doc: ParsedDocumentation):
        _check_duplicates(doc, self.docs)
        self.docs[doc.unique_id] = doc
        source_file.docs.append(doc.unique_id)

    # end of methods formerly in ParseResult

    # Provide support for copy.deepcopy() - we just need to avoid the lock!
    # pickle and deepcopy use this. It returns a callable object used to
    # create the initial version of the object and a tuple of arguments
    # for the object, i.e. the Manifest.
    # The order of the arguments must match the order of the attributes
    # in the Manifest class declaration, because they are used as
    # positional arguments to construct a Manifest.
    def __reduce_ex__(self, protocol):
        args = (
            self.nodes,
            self.sources,
            self.macros,
            self.docs,
            self.exposures,
            self.selectors,
            self.files,
            self.metadata,
            self.flat_graph,
            self.state_check,
            self.source_patches,
            self.disabled,
            self.env_vars,
            self._doc_lookup,
            self._source_lookup,
            self._ref_lookup,
            self._disabled_lookup,
            self._analysis_lookup,
        )
        return self.__class__, args


class MacroManifest(MacroMethods):
    def __init__(self, macros):
        self.macros = macros
        self.metadata = ManifestMetadata()
        # This is returned by the 'graph' context property
        # in the ProviderContext class.
        self.flat_graph = {}


AnyManifest = Union[Manifest, MacroManifest]


@dataclass
@schema_version('manifest', 3)
class WritableManifest(ArtifactMixin):
    nodes: Mapping[UniqueID, ManifestNode] = field(
        metadata=dict(description=(
            'The nodes defined in the dbt project and its dependencies'
        ))
    )
    sources: Mapping[UniqueID, ParsedSourceDefinition] = field(
        metadata=dict(description=(
            'The sources defined in the dbt project and its dependencies'
        ))
    )
    macros: Mapping[UniqueID, ParsedMacro] = field(
        metadata=dict(description=(
            'The macros defined in the dbt project and its dependencies'
        ))
    )
    docs: Mapping[UniqueID, ParsedDocumentation] = field(
        metadata=dict(description=(
            'The docs defined in the dbt project and its dependencies'
        ))
    )
    exposures: Mapping[UniqueID, ParsedExposure] = field(
        metadata=dict(description=(
            'The exposures defined in the dbt project and its dependencies'
        ))
    )
    selectors: Mapping[UniqueID, Any] = field(
        metadata=dict(description=(
            'The selectors defined in selectors.yml'
        ))
    )
    disabled: Optional[Mapping[UniqueID, List[CompileResultNode]]] = field(metadata=dict(
        description='A mapping of the disabled nodes in the target'
    ))
    parent_map: Optional[NodeEdgeMap] = field(metadata=dict(
        description='A mapping from child nodes to their dependencies',
    ))
    child_map: Optional[NodeEdgeMap] = field(metadata=dict(
        description='A mapping from parent nodes to their dependents',
    ))
    metadata: ManifestMetadata = field(metadata=dict(
        description='Metadata about the manifest',
    ))


def _check_duplicates(
    value: HasUniqueID, src: Mapping[str, HasUniqueID]
):
    if value.unique_id in src:
        raise_duplicate_resource_name(value, src[value.unique_id])


K_T = TypeVar('K_T')
V_T = TypeVar('V_T')


def _expect_value(
    key: K_T, src: Mapping[K_T, V_T], old_file: SourceFile, name: str
) -> V_T:
    if key not in src:
        raise CompilationException(
            'Expected to find "{}" in cached "result.{}" based '
            'on cached file information: {}!'
            .format(key, name, old_file)
        )
    return src[key]
