import abc
import enum
import hashlib
import os
from dataclasses import dataclass, field
from datetime import datetime
from itertools import chain
from multiprocessing.synchronize import Lock
from typing import (
    Dict, List, Optional, Union, Mapping, MutableMapping, Any, Set, Tuple,
    TypeVar, Callable, Iterable, Generic, cast
)
from typing_extensions import Protocol
from uuid import UUID

from hologram import JsonSchemaMixin

from dbt.contracts.graph.compiled import (
    CompileResultNode, NonSourceNode, NonSourceCompiledNode
)
from dbt.contracts.graph.parsed import (
    ParsedMacro, ParsedDocumentation, ParsedNodePatch, ParsedMacroPatch,
    ParsedSourceDefinition
)
from dbt.contracts.util import Readable, Writable, Replaceable
from dbt.exceptions import (
    raise_duplicate_resource_name, InternalException, raise_compiler_error,
    warn_or_error, raise_invalid_patch
)
from dbt.helper_types import PathSet
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType
from dbt import deprecations
from dbt import flags
from dbt import tracking
import dbt.utils

NodeEdgeMap = Dict[str, List[str]]
MacroKey = Tuple[str, str]
SourceKey = Tuple[str, str]
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
            self.add_source(source)

    def perform_lookup(
        self, unique_id: UniqueID
    ) -> ParsedSourceDefinition:
        if unique_id not in self._manifest.sources:
            raise dbt.exceptions.InternalException(
                f'Source {unique_id} found in cache but not found in manifest'
            )
        return self._manifest.sources[unique_id]


class RefableCache(PackageAwareCache[RefName, NonSourceNode]):
    # refables are actually unique, so the Dict[PackageName, UniqueID] will
    # only ever have exactly one value, but doing 3 dict lookups instead of 1
    # is not a big deal at all and retains consistency
    def __init__(self, manifest: 'Manifest'):
        self._cached_types = set(NodeType.refable())
        super().__init__(manifest)

    def add_node(self, node: NonSourceNode):
        if node.resource_type in self._cached_types:
            if node.name not in self.storage:
                self.storage[node.name] = {}
            self.storage[node.name][node.package_name] = node.unique_id

    def populate(self):
        for node in self._manifest.nodes.values():
            self.add_node(node)

    def perform_lookup(
        self, unique_id: UniqueID
    ) -> NonSourceNode:
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
    # any macro patches in this file. The entries are package, name pairs.
    macro_patches: List[MacroKey] = field(default_factory=list)
    # any source patches in this file. The entries are package, name pairs
    source_patches: List[SourceKey] = field(default_factory=list)

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
    """Metadata for the manifest."""
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


def _sort_values(dct):
    """Given a dictionary, sort each value. This makes output deterministic,
    which helps for tests.
    """
    return {k: sorted(v) for k, v in dct.items()}


def build_edges(nodes: List[NonSourceNode]):
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
    NonSourceNode,
    Disabled[NonSourceNode]
]]


T = TypeVar('T', bound=CompileResultNode)


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


@dataclass
class Manifest:
    """The manifest for the full graph, after parsing and during compilation.
    """
    nodes: MutableMapping[str, NonSourceNode]
    sources: MutableMapping[str, ParsedSourceDefinition]
    macros: MutableMapping[str, ParsedMacro]
    docs: MutableMapping[str, ParsedDocumentation]
    generated_at: datetime
    disabled: List[CompileResultNode]
    files: MutableMapping[str, SourceFile]
    metadata: ManifestMetadata = field(default_factory=ManifestMetadata)
    flat_graph: Dict[str, Any] = field(default_factory=dict)
    _docs_cache: Optional[DocCache] = None
    _sources_cache: Optional[SourceCache] = None
    _refs_cache: Optional[RefableCache] = None
    _lock: Lock = field(default_factory=flags.MP_CONTEXT.Lock)

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
            sources={},
            macros=macros,
            docs={},
            generated_at=datetime.utcnow(),
            disabled=[],
            files=files,
        )

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

    def update_node(self, new_node: NonSourceNode):
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
            'nodes': {
                k: v.to_dict(omit_none=False) for k, v in self.nodes.items()
            },
            'sources': {
                k: v.to_dict(omit_none=False) for k, v in self.sources.items()
            }
        }

    def find_disabled_by_name(
        self, name: str, package: Optional[str] = None
    ) -> Optional[NonSourceNode]:
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

    def get_resource_fqns(self) -> Mapping[str, PathSet]:
        resource_fqns: Dict[str, Set[Tuple[str, ...]]] = {}
        all_resources = chain(self.nodes.values(), self.sources.values())
        for resource in all_resources:
            resource_type_plural = resource.resource_type.pluralize()
            if resource_type_plural not in resource_fqns:
                resource_fqns[resource_type_plural] = set()
            resource_fqns[resource_type_plural].add(tuple(resource.fqn))
        return resource_fqns

    def add_nodes(self, new_nodes: Mapping[str, NonSourceNode]):
        """Add the given dict of new nodes to the manifest."""
        for unique_id, node in new_nodes.items():
            if unique_id in self.nodes:
                raise_duplicate_resource_name(node, self.nodes[unique_id])
            self.nodes[unique_id] = node
            # fixup the cache if it exists.
            if self._refs_cache is not None:
                if node.resource_type in NodeType.refable():
                    self._refs_cache.add_node(node)

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
                    raise_invalid_patch(
                        node, patch.yaml_key, patch.original_file_path
                    )

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
            generated_at=self.generated_at,
            disabled=[_deepcopy(n) for n in self.disabled],
            metadata=self.metadata,
            files={k: _deepcopy(v) for k, v in self.files.items()},
        )

    def writable_manifest(self):
        edge_members = list(chain(self.nodes.values(), self.sources.values()))
        forward_edges, backward_edges = build_edges(edge_members)

        return WritableManifest(
            nodes=self.nodes,
            sources=self.sources,
            macros=self.macros,
            docs=self.docs,
            generated_at=self.generated_at,
            metadata=self.metadata,
            disabled=self.disabled,
            child_map=forward_edges,
            parent_map=backward_edges,
        )

    def to_dict(self, omit_none=True, validate=False):
        return self.writable_manifest().to_dict(
            omit_none=omit_none, validate=validate
        )

    def write(self, path):
        self.writable_manifest().write(path)

    def expect(self, unique_id: str) -> CompileResultNode:
        if unique_id in self.nodes:
            return self.nodes[unique_id]
        elif unique_id in self.sources:
            return self.sources[unique_id]
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

        node: Optional[NonSourceNode] = None
        disabled: Optional[NonSourceNode] = None

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
        other: 'WritableManifest',
        selected: Set[UniqueID],
    ) -> None:
        """Given the selected unique IDs and a writable manifest, update this
        manifest by replacing any unselected nodes with their counterpart.

        Only non-ephemeral refable nodes are examined.
        """
        refables = set(NodeType.refable())
        for unique_id, node in other.nodes.items():
            if (
                node.resource_type in refables and
                not node.is_ephemeral and
                unique_id not in selected
            ):
                self.nodes[unique_id] = node


@dataclass
class WritableManifest(JsonSchemaMixin, Writable, Readable):
    nodes: Mapping[UniqueID, NonSourceNode] = field(
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
    disabled: Optional[List[CompileResultNode]] = field(metadata=dict(
        description='A list of the disabled nodes in the target'
    ))
    generated_at: datetime = field(metadata=dict(
        description='The time at which the manifest was generated',
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
