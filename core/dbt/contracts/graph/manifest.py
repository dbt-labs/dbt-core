import enum
import hashlib
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Union, Mapping, Any
from uuid import UUID

from hologram import JsonSchemaMixin

from dbt.contracts.graph.parsed import ParsedNode, ParsedMacro, \
    ParsedDocumentation
from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.util import Writable, Replaceable
from dbt.exceptions import (
    raise_duplicate_resource_name, InternalException, raise_compiler_error
)
from dbt.include.global_project import PACKAGES
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType
from dbt.ui import printer
from dbt import deprecations
from dbt import tracking
import dbt.utils

NodeEdgeMap = Dict[str, List[str]]


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
        if not isinstance(other, type(self)):
            return False

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
    backward_edges = {}
    # pre-populate the forward edge dict for simplicity
    forward_edges = {node.unique_id: [] for node in nodes}
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
class MaterializationCandidate:
    specificity: Specificity
    locality: Locality
    macro: ParsedMacro

    def __eq__(self, other):
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

    def __lt__(self, other: 'MaterializationCandidate') -> bool:
        if self.specificity < other.specificity:
            return True
        if self.specificity > other.specificity:
            return False
        if self.locality < other.locality:
            return True
        if self.locality > other.locality:
            return False
        return False


@dataclass
class Manifest:
    """The manifest for the full graph, after parsing and during compilation.
    """
    nodes: Mapping[str, CompileResultNode]
    macros: Mapping[str, ParsedMacro]
    docs: Mapping[str, ParsedDocumentation]
    generated_at: datetime
    disabled: List[ParsedNode]
    files: Mapping[str, SourceFile]
    metadata: ManifestMetadata = field(default_factory=ManifestMetadata)
    flat_graph: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_macros(cls, macros=None, files=None) -> 'Manifest':
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

    def find_disabled_by_name(self, name, package=None):
        return dbt.utils.find_in_list_by_name(self.disabled, name, package,
                                              NodeType.refable())

    def _find_by_name(self, name, package, subgraph, nodetype):
        """
        Find a node by its given name in the appropriate sugraph. If package is
        None, all pacakges will be searched.
        nodetype should be a list of NodeTypes to accept.
        """
        if subgraph == 'nodes':
            search = self.nodes
        elif subgraph == 'macros':
            search = self.macros
        else:
            raise NotImplementedError(
                'subgraph search for {} not implemented'.format(subgraph)
            )
        return dbt.utils.find_in_subgraph_by_name(
            search,
            name,
            package,
            nodetype)

    def find_docs_by_name(self, name, package=None):
        for unique_id, doc in self.docs.items():
            parts = unique_id.split('.')
            if len(parts) != 2:
                msg = "documentation names cannot contain '.' characters"
                raise_compiler_error(msg, doc)

            found_package, found_node = parts

            if (name == found_node and package in {None, found_package}):
                return doc
        return None

    def find_macro_by_name(self, name, package):
        """Find a macro in the graph by its name and package name, or None for
        any package.
        """
        return self._find_by_name(name, package, 'macros', [NodeType.Macro])

    def find_refable_by_name(self, name, package):
        """Find any valid target for "ref()" in the graph by its name and
        package name, or None for any package.
        """
        return self._find_by_name(name, package, 'nodes', NodeType.refable())

    def find_source_by_name(self, source_name, table_name, package):
        """Find any valid target for "source()" in the graph by its name and
        package name, or None for any package.
        """
        name = '{}.{}'.format(source_name, table_name)
        return self._find_by_name(name, package, 'nodes', [NodeType.Source])

    def get_materialization_macro(
        self, project_name: str, materialization_name: str, adapter_type: str
    ):
        adapter_macro_name, default_macro_name = [
            dbt.utils.get_materialization_macro_name(
                materialization_name=materialization_name,
                adapter_type=atype,
                with_prefix=False,
            )
            for atype in (adapter_type, None)
        ]

        candidates: List[MaterializationCandidate] = []

        for unique_id, macro in self.macros.items():
            specificity: Specificity
            locality: Locality
            if macro.name == adapter_macro_name:
                specificity = Specificity.Adapter
            elif macro.name == default_macro_name:
                specificity = Specificity.Default
            else:
                continue

            if macro.package_name == project_name:
                locality = Locality.Root
            elif macro.package_name in PACKAGES:
                locality = Locality.Core
            else:
                locality = Locality.Imported

            candidate = MaterializationCandidate(
                specificity=specificity,
                locality=locality,
                macro=macro,
            )
            candidates.append(candidate)

        if not candidates:
            return None
        candidates.sort()
        return candidates[-1].macro

    def get_resource_fqns(self):
        resource_fqns = {}
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

    def patch_nodes(self, patches):
        """Patch nodes with the given dict of patches. Note that this consumes
        the input!
        This relies on the fact that all nodes have unique _name_ fields, not
        just unique unique_id fields.
        """
        # because we don't have any mapping from node _names_ to nodes, and we
        # only have the node name in the patch, we have to iterate over all the
        # nodes looking for matching names. We could use _find_by_name if we
        # were ok with doing an O(n*m) search (one nodes scan per patch)
        for node in self.nodes.values():
            if node.resource_type == NodeType.Source:
                continue
            patch = patches.pop(node.name, None)
            if not patch:
                continue
            expected_key = node.resource_type.pluralize()
            if expected_key == patch.yaml_key:
                node.patch(patch)
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
                    'WARNING: Found documentation for model "{}" which was '
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
