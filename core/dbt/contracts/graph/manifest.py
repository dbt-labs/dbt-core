import abc
import enum
from dataclasses import dataclass, field
from itertools import chain, islice
from multiprocessing.synchronize import Lock
from typing import (
    Dict, List, Optional, Union, Mapping, MutableMapping, Any, Set, Tuple,
    TypeVar, Callable, Iterable, Generic, cast, AbstractSet
)
from typing_extensions import Protocol
from uuid import UUID

from dbt.contracts.graph.compiled import (
    CompileResultNode, ManifestNode, NonSourceCompiledNode, GraphMemberNode
)
from dbt.contracts.graph.parsed import (
    ParsedMacro, ParsedDocumentation, ParsedNodePatch, ParsedMacroPatch,
    ParsedSourceDefinition, ParsedExposure, HasUniqueID,
    UnpatchedSourceDefinition, ManifestNodes
)
from dbt.contracts.graph.unparsed import SourcePatch
from dbt.contracts.files import SourceFile, FileHash, RemoteFile
from dbt.contracts.util import (
    BaseArtifactMetadata, MacroKey, SourceKey, ArtifactMixin, schema_version
)
from dbt.exceptions import (
    InternalException, CompilationException,
    raise_duplicate_resource_name, raise_compiler_error, warn_or_error,
    raise_invalid_patch, raise_duplicate_patch_name,
    raise_duplicate_macro_patch_name, raise_duplicate_source_patch_name,
)
from dbt.helper_types import PathSet
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType
from dbt.ui import line_wrap_message
from dbt import deprecations
from dbt import flags
from dbt import tracking
import dbt.utils

NodeEdgeMap = Dict[str, List[str]]
PackageName = str
DocName = str
RefName = str
UniqueID = str


K_T = TypeVar('K_T')
V_T = TypeVar('V_T')


class PackageAwareCache(Generic[K_T, V_T]):
    def __init__(self, manifest: 'Manifest'):
        self.storage: Dict[K_T, Dict[PackageName, UniqueID]] = {}
        self._manifest = manifest
        self.populate()

    @abc.abstractmethod
    def populate(self):
        pass

    @abc.abstractmethod
    def perform_lookup(self, unique_id: UniqueID) -> V_T:
        pass

    def find_cached_value(
        self, key: K_T, package: Optional[PackageName]
    ) -> Optional[V_T]:
        unique_id = self.find_unique_id_for_package(key, package)
        if unique_id is not None:
            return self.perform_lookup(unique_id)
        return None

    def find_unique_id_for_package(
        self, key: K_T, package: Optional[PackageName]
    ) -> Optional[UniqueID]:
        if key not in self.storage:
            return None

        pkg_dct: Mapping[PackageName, UniqueID] = self.storage[key]

        if package is None:
            if not pkg_dct:
                return None
            else:
                return next(iter(pkg_dct.values()))
        elif package in pkg_dct:
            return pkg_dct[package]
        else:
            return None


class DocCache(PackageAwareCache[DocName, ParsedDocumentation]):
    def add_doc(self, doc: ParsedDocumentation):
        if doc.name not in self.storage:
            self.storage[doc.name] = {}
        self.storage[doc.name][doc.package_name] = doc.unique_id

    def populate(self):
        for doc in self._manifest.docs.values():
            self.add_doc(doc)

    def perform_lookup(
        self, unique_id: UniqueID
    ) -> ParsedDocumentation:
        if unique_id not in self._manifest.docs:
            raise dbt.exceptions.InternalException(
                f'Doc {unique_id} found in cache but not found in manifest'
            )
        return self._manifest.docs[unique_id]


class SourceCache(PackageAwareCache[SourceKey, ParsedSourceDefinition]):
    def add_source(self, source: ParsedSourceDefinition):
        key = (source.source_name, source.name)
        if key not in self.storage:
            self.storage[key] = {}

        self.storage[key][source.package_name] = source.unique_id

    def populate(self):
        for source in self._manifest.sources.values():
            if hasattr(source, 'source_name'):
                self.add_source(source)

    def perform_lookup(
        self, unique_id: UniqueID
    ) -> ParsedSourceDefinition:
        if unique_id not in self._manifest.sources:
            raise dbt.exceptions.InternalException(
                f'Source {unique_id} found in cache but not found in manifest'
            )
        return self._manifest.sources[unique_id]


class RefableCache(PackageAwareCache[RefName, ManifestNode]):
    # refables are actually unique, so the Dict[PackageName, UniqueID] will
    # only ever have exactly one value, but doing 3 dict lookups instead of 1
    # is not a big deal at all and retains consistency
    def __init__(self, manifest: 'Manifest'):
        self._cached_types = set(NodeType.refable())
        super().__init__(manifest)

    def add_node(self, node: ManifestNode):
        if node.resource_type in self._cached_types:
            if node.name not in self.storage:
                self.storage[node.name] = {}
            self.storage[node.name][node.package_name] = node.unique_id

    def populate(self):
        for node in self._manifest.nodes.values():
            self.add_node(node)

    def perform_lookup(
        self, unique_id: UniqueID
    ) -> ManifestNode:
        if unique_id not in self._manifest.nodes:
            raise dbt.exceptions.InternalException(
                f'Node {unique_id} found in cache but not found in manifest'
            )
        return self._manifest.nodes[unique_id]


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
            self.send_anonymous_usage_stats = (
                not tracking.active_user.do_not_track
            )

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


def build_edges(nodes: List[ManifestNode]):
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
class ManifestStateCheck():
    vars_hash: FileHash
    profile_hash: FileHash
    project_hashes: MutableMapping[str, FileHash]


@dataclass
class Manifest(MacroMethods):
    """The manifest for the full graph, after parsing and during compilation.
    """
    # These attributes are both positional and by keyword. If an attribute
    # is added it must all be added in the __reduce_ex__ method in the
    # args tuple in the right position.
    nodes: MutableMapping[str, ManifestNode]
    sources: MutableMapping[str, ParsedSourceDefinition]
    macros: MutableMapping[str, ParsedMacro]
    docs: MutableMapping[str, ParsedDocumentation]
    exposures: MutableMapping[str, ParsedExposure]
    selectors: MutableMapping[str, Any]
    disabled: List[CompileResultNode]
    files: MutableMapping[str, SourceFile]
    metadata: ManifestMetadata = field(default_factory=ManifestMetadata)
    flat_graph: Dict[str, Any] = field(default_factory=dict)
    state_check: Optional[ManifestStateCheck] = None
    # Moved from the ParseResult object
    macro_patches: MutableMapping[MacroKey, ParsedMacroPatch] = field(default_factory=dict)
    patches: MutableMapping[str, ParsedNodePatch] = field(default_factory=dict)
    source_patches: MutableMapping[SourceKey, SourcePatch] = field(default_factory=dict)
    # following is from ParseResult
    _disabled: MutableMapping[str, List[CompileResultNode]] = field(default_factory=dict)
    _docs_cache: Optional[DocCache] = None
    _sources_cache: Optional[SourceCache] = None
    _refs_cache: Optional[RefableCache] = None
    _lock: Lock = field(default_factory=flags.MP_CONTEXT.Lock)

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

    def find_disabled_by_name(
        self, name: str, package: Optional[str] = None
    ) -> Optional[ManifestNode]:
        searcher: NameSearcher = NameSearcher(
            name, package, NodeType.refable()
        )
        result = searcher.search(self.disabled)
        return result

    def find_disabled_source_by_name(
        self, source_name: str, table_name: str, package: Optional[str] = None
    ) -> Optional[ParsedSourceDefinition]:
        search_name = f'{source_name}.{table_name}'
        searcher: NameSearcher = NameSearcher(
            search_name, package, [NodeType.Source]
        )
        result = searcher.search(self.disabled)
        if result is not None:
            assert isinstance(result, ParsedSourceDefinition)
        return result

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

    def add_nodes(self, new_nodes: Mapping[str, ManifestNode]):
        """Add the given dict of new nodes to the manifest."""
        for unique_id, node in new_nodes.items():
            if unique_id in self.nodes:
                raise_duplicate_resource_name(node, self.nodes[unique_id])
            self.nodes[unique_id] = node
            # fixup the cache if it exists.
            if self._refs_cache is not None:
                if node.resource_type in NodeType.refable():
                    self._refs_cache.add_node(node)

    def add_patch(
        self, source_file: SourceFile, patch: ParsedNodePatch,
    ) -> None:
        # patches can't be overwritten
        if patch.name in self.patches:
            raise_duplicate_patch_name(patch, self.patches[patch.name])
        self.patches[patch.name] = patch
        self.get_file(source_file).patches.append(patch.name)

    def add_macro_patch(
        self, source_file: SourceFile, patch: ParsedMacroPatch,
    ) -> None:
        # macros are fully namespaced
        key = (patch.package_name, patch.name)
        if key in self.macro_patches:
            raise_duplicate_macro_patch_name(patch, self.macro_patches[key])
        self.macro_patches[key] = patch
        self.get_file(source_file).macro_patches.append(key)

    def add_source_patch(
        self, source_file: SourceFile, patch: SourcePatch,
    ) -> None:
        # source patches must be unique
        key = (patch.overrides, patch.name)
        if key in self.source_patches:
            raise_duplicate_source_patch_name(patch, self.source_patches[key])
        self.source_patches[key] = patch
        self.get_file(source_file).source_patches.append(key)

    def patch_macros(self) -> None:
        for macro in self.macros.values():
            key = (macro.package_name, macro.name)
            patch = self.macro_patches.pop(key, None)
            if not patch:
                continue
            macro.patch(patch)

        if self.macro_patches:
            for patch in self.macro_patches.values():
                warn_or_error(
                    f'WARNING: Found documentation for macro "{patch.name}" '
                    f'which was not found'
                )

    def patch_nodes(self) -> None:
        """Patch nodes with the given dict of patches. Note that this consumes
        the input!
        This relies on the fact that all nodes have unique _name_ fields, not
        just unique unique_id fields.
        """
        # because we don't have any mapping from node _names_ to nodes, and we
        # only have the node name in the patch, we have to iterate over all the
        # nodes looking for matching names. We could use a NameSearcher if we
        # were ok with doing an O(n*m) search (one nodes scan per patch)
        # Q: could we save patches by node unique_ids instead, or convert
        # between names and node ids?
        for node in self.nodes.values():
            patch = self.patches.pop(node.name, None)
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
                    raise_invalid_patch(
                        node, patch.yaml_key, patch.original_file_path
                    )

            node.patch(patch)

        # If anything is left in self.patches, it means that the node for
        # that patch wasn't found.
        if self.patches:
            for patch in self.patches.values():
                # since patches aren't nodes, we can't use the existing
                # target_not_found warning
                logger.debug((
                    'WARNING: Found documentation for resource "{}" which was '
                    'not found or is disabled').format(patch.name)
                )

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

    # This is used in dbt.task.rpc.sql_commands 'add_new_refs'
    def deepcopy(self):
        return Manifest(
            nodes={k: _deepcopy(v) for k, v in self.nodes.items()},
            sources={k: _deepcopy(v) for k, v in self.sources.items()},
            macros={k: _deepcopy(v) for k, v in self.macros.items()},
            docs={k: _deepcopy(v) for k, v in self.docs.items()},
            exposures={k: _deepcopy(v) for k, v in self.exposures.items()},
            selectors=self.root_project.manifest_selectors,
            metadata=self.metadata,
            disabled=[_deepcopy(n) for n in self.disabled],
            files={k: _deepcopy(v) for k, v in self.files.items()},
        )

    def writable_manifest(self):
        edge_members = list(chain(
            self.nodes.values(),
            self.sources.values(),
            self.exposures.values(),
        ))
        forward_edges, backward_edges = build_edges(edge_members)

        return WritableManifest(
            nodes=self.nodes,
            sources=self.sources,
            macros=self.macros,
            docs=self.docs,
            exposures=self.exposures,
            selectors=self.selectors,
            metadata=self.metadata,
            disabled=self.disabled,
            child_map=forward_edges,
            parent_map=backward_edges,
        )

    # When 'to_dict' is called on the Manifest, it substitues a
    # WritableManifest
    def __pre_serialize__(self):
        return self.writable_manifest()

    def write(self, path):
        self.writable_manifest().write(path)

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
    def docs_cache(self) -> DocCache:
        if self._docs_cache is not None:
            return self._docs_cache
        cache = DocCache(self)
        self._docs_cache = cache
        return cache

    @property
    def source_cache(self) -> SourceCache:
        if self._sources_cache is not None:
            return self._sources_cache
        cache = SourceCache(self)
        self._sources_cache = cache
        return cache

    @property
    def refs_cache(self) -> RefableCache:
        if self._refs_cache is not None:
            return self._refs_cache
        cache = RefableCache(self)
        self._refs_cache = cache
        return cache

    def resolve_ref(
        self,
        target_model_name: str,
        target_model_package: Optional[str],
        current_project: str,
        node_package: str,
    ) -> MaybeNonSource:

        node: Optional[ManifestNode] = None
        disabled: Optional[ManifestNode] = None

        candidates = _search_packages(
            current_project, node_package, target_model_package
        )
        for pkg in candidates:
            node = self.refs_cache.find_cached_value(target_model_name, pkg)

            if node is not None and node.config.enabled:
                return node

            # it's possible that the node is disabled
            if disabled is None:
                disabled = self.find_disabled_by_name(
                    target_model_name, pkg
                )

        if disabled is not None:
            return Disabled(disabled)
        return None

    def resolve_source(
        self,
        target_source_name: str,
        target_table_name: str,
        current_project: str,
        node_package: str
    ) -> MaybeParsedSource:
        key = (target_source_name, target_table_name)
        candidates = _search_packages(current_project, node_package)

        source: Optional[ParsedSourceDefinition] = None
        disabled: Optional[ParsedSourceDefinition] = None

        for pkg in candidates:
            source = self.source_cache.find_cached_value(key, pkg)
            if source is not None and source.config.enabled:
                return source

            if disabled is None:
                disabled = self.find_disabled_source_by_name(
                    target_source_name, target_table_name, pkg
                )

        if disabled is not None:
            return Disabled(disabled)
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
        candidates = _search_packages(
            current_project, node_package, package
        )

        for pkg in candidates:
            result = self.docs_cache.find_cached_value(name, pkg)
            if result is not None:
                return result
        return None

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
    def get_file(self, source_file: SourceFile) -> SourceFile:
        key = source_file.search_key
        if key is None:
            return source_file
        if key not in self.files:
            self.files[key] = source_file
        return self.files[key]

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
        self.get_file(source_file).macros.append(macro.unique_id)

    def has_file(self, source_file: SourceFile) -> bool:
        key = source_file.search_key
        if key is None:
            return False
        if key not in self.files:
            return False
        my_checksum = self.files[key].checksum
        return my_checksum == source_file.checksum

    def add_source(
        self, source_file: SourceFile, source: UnpatchedSourceDefinition
    ):
        # sources can't be overwritten!
        _check_duplicates(source, self.sources)
        self.sources[source.unique_id] = source  # type: ignore
        self.get_file(source_file).sources.append(source.unique_id)

    def add_node_nofile(self, node: ManifestNodes):
        # nodes can't be overwritten!
        _check_duplicates(node, self.nodes)
        self.nodes[node.unique_id] = node

    def add_node(self, source_file: SourceFile, node: ManifestNodes):
        self.add_node_nofile(node)
        self.get_file(source_file).nodes.append(node.unique_id)

    def add_exposure(self, source_file: SourceFile, exposure: ParsedExposure):
        _check_duplicates(exposure, self.exposures)
        self.exposures[exposure.unique_id] = exposure
        self.get_file(source_file).exposures.append(exposure.unique_id)

    def add_disabled_nofile(self, node: CompileResultNode):
        if node.unique_id in self._disabled:
            self._disabled[node.unique_id].append(node)
        else:
            self._disabled[node.unique_id] = [node]

    def add_disabled(self, source_file: SourceFile, node: CompileResultNode):
        self.add_disabled_nofile(node)
        self.get_file(source_file).nodes.append(node.unique_id)

    def add_doc(self, source_file: SourceFile, doc: ParsedDocumentation):
        _check_duplicates(doc, self.docs)
        self.docs[doc.unique_id] = doc
        self.get_file(source_file).docs.append(doc.unique_id)

    def _get_disabled(
        self,
        unique_id: str,
        match_file: SourceFile,
    ) -> List[CompileResultNode]:
        if unique_id not in self._disabled:
            raise InternalException(
                'called _get_disabled with id={}, but it does not exist'
                .format(unique_id)
            )
        return [
            n for n in self._disabled[unique_id]
            if n.original_file_path == match_file.path.original_file_path
        ]

    # This is only used by 'sanitized_update' which processes "old_manifest"
    def _process_node(
        self,
        node_id: str,
        source_file: SourceFile,
        old_file: SourceFile,
        old_manifest: Any,
    ) -> None:
        """Nodes are a special kind of complicated - there can be multiple
        with the same name, as long as all but one are disabled.

        Only handle nodes where the matching node has the same resource type
        as the current parser.
        """
        source_path = source_file.path.original_file_path
        found: bool = False
        if node_id in old_manifest.nodes:
            old_node = old_manifest.nodes[node_id]
            if old_node.original_file_path == source_path:
                self.add_node(source_file, old_node)
                found = True

        if node_id in old_manifest._disabled:
            matches = old_manifest._get_disabled(node_id, source_file)
            for match in matches:
                self.add_disabled(source_file, match)
                found = True

        if not found:
            raise CompilationException(
                'Expected to find "{}" in cached "manifest.nodes" or '
                '"manifest.disabled" based on cached file information: {}!'
                .format(node_id, old_file)
            )

    # This is called by ManifestLoader._get_cached/parse_with_cache,
    # which handles updating the ManifestLoader results with information
    # from the "old_manifest", i.e. the pickle file if the checksums are
    # the same.
    def sanitized_update(
        self,
        source_file: SourceFile,
        old_manifest: Any,
        resource_type: NodeType,
    ) -> bool:

        if isinstance(source_file.path, RemoteFile):
            return False

        old_file = old_manifest.get_file(source_file)
        for doc_id in old_file.docs:
            doc = _expect_value(doc_id, old_manifest.docs, old_file, "docs")
            self.add_doc(source_file, doc)

        for macro_id in old_file.macros:
            macro = _expect_value(
                macro_id, old_manifest.macros, old_file, "macros"
            )
            self.add_macro(source_file, macro)

        for source_id in old_file.sources:
            source = _expect_value(
                source_id, old_manifest.sources, old_file, "sources"
            )
            self.add_source(source_file, source)

        # because we know this is how we _parsed_ the node, we can safely
        # assume if it's disabled it was done by the project or file, and
        # we can keep our old data
        # the node ID could be in old_manifest.disabled AND in old_manifest.nodes.
        # In that case, we have to make sure the path also matches.
        for node_id in old_file.nodes:
            # cheat: look at the first part of the node ID and compare it to
            # the parser resource type. On a mismatch, bail out.
            if resource_type != node_id.split('.')[0]:
                continue
            self._process_node(node_id, source_file, old_file, old_manifest)

        for exposure_id in old_file.exposures:
            exposure = _expect_value(
                exposure_id, old_manifest.exposures, old_file, "exposures"
            )
            self.add_exposure(source_file, exposure)

        # Note: There shouldn't be any patches in here after the cleanup.
        # The pickled Manifest should have had all patches applied.
        patched = False
        for name in old_file.patches:
            patch = _expect_value(
                name, old_manifest.patches, old_file, "patches"
            )
            self.add_patch(source_file, patch)
            patched = True
        if patched:
            self.get_file(source_file).patches.sort()

        macro_patched = False
        for key in old_file.macro_patches:
            macro_patch = _expect_value(
                key, old_manifest.macro_patches, old_file, "macro_patches"
            )
            self.add_macro_patch(source_file, macro_patch)
            macro_patched = True
        if macro_patched:
            self.get_file(source_file).macro_patches.sort()

        return True
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
            self.disabled,
            self.files,
            self.metadata,
            self.flat_graph,
            self.state_check,
            self.macro_patches,
            self.patches,
            self.source_patches,
            self._disabled,
            self._docs_cache,
            self._sources_cache,
            self._refs_cache,
        )
        return self.__class__, args


class MacroManifest(MacroMethods):
    def __init__(self, macros, files):
        self.macros = macros
        self.files = files
        self.metadata = ManifestMetadata()
        # This is returned by the 'graph' context property
        # in the ProviderContext class.
        self.flat_graph = {}


AnyManifest = Union[Manifest, MacroManifest]


@dataclass
@schema_version('manifest', 1)
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
    disabled: Optional[List[CompileResultNode]] = field(metadata=dict(
        description='A list of the disabled nodes in the target'
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
