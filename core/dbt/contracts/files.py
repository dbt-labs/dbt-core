import hashlib
import os
from dataclasses import dataclass, field
from mashumaro.types import SerializableType
from typing import List, Optional, Union, Dict, Any

from dbt.dataclass_schema import dbtClassMixin, StrEnum

from .util import SourceKey


MAXIMUM_SEED_SIZE = 1 * 1024 * 1024
MAXIMUM_SEED_SIZE_NAME = '1MB'


class ParseFileType(StrEnum):
    Macro = 'macro'
    Model = 'model'
    Snapshot = 'snapshot'
    Analysis = 'analysis'
    Test = 'test'
    Seed = 'seed'
    Documentation = 'docs'
    Schema = 'schema'
    Hook = 'hook'   # not a real filetype, from dbt_project.yml


parse_file_type_to_parser = {
    ParseFileType.Macro: 'MacroParser',
    ParseFileType.Model: 'ModelParser',
    ParseFileType.Snapshot: 'SnapshotParser',
    ParseFileType.Analysis: 'AnalysisParser',
    ParseFileType.Test: 'DataTestParser',
    ParseFileType.Seed: 'SeedParser',
    ParseFileType.Documentation: 'DocumentationParser',
    ParseFileType.Schema: 'SchemaParser',
    ParseFileType.Hook: 'HookParser',
}


@dataclass
class FilePath(dbtClassMixin):
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

    def seed_too_large(self) -> bool:
        """Return whether the file this represents is over the seed size limit
        """
        return os.stat(self.full_path).st_size > MAXIMUM_SEED_SIZE


@dataclass
class FileHash(dbtClassMixin):
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
    def from_contents(cls, contents: str, name='sha256') -> 'FileHash':
        """Create a file hash from the given file contents. The hash is always
        the utf-8 encoding of the contents given, because dbt only reads files
        as utf-8.
        """
        data = contents.encode('utf-8')
        checksum = hashlib.new(name, data).hexdigest()
        return cls(name=name, checksum=checksum)


@dataclass
class RemoteFile(dbtClassMixin):
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
class BaseSourceFile(dbtClassMixin, SerializableType):
    """Define a source file in dbt"""
    path: Union[FilePath, RemoteFile]  # the path information
    checksum: FileHash
    # Seems like knowing which project the file came from would be useful
    project_name: Optional[str] = None
    # Parse file type: i.e. which parser will process this file
    parse_file_type: Optional[ParseFileType] = None
    # we don't want to serialize this
    contents: Optional[str] = None
    # the unique IDs contained in this file

    @property
    def file_id(self):
        if isinstance(self.path, RemoteFile):
            return None
        if self.checksum.name == 'none':
            return None
        return f'{self.project_name}://{self.path.original_file_path}'

    def _serialize(self):
        dct = self.to_dict()
        return dct

    @classmethod
    def _deserialize(cls, dct: Dict[str, int]):
        if dct['parse_file_type'] == 'schema':
            sf = SchemaSourceFile.from_dict(dct)
        else:
            sf = SourceFile.from_dict(dct)
        return sf

    def __post_serialize__(self, dct):
        dct = super().__post_serialize__(dct)
        # remove empty lists to save space
        dct_keys = list(dct.keys())
        for key in dct_keys:
            if isinstance(dct[key], list) and not dct[key]:
                del dct[key]
        # remove contents. Schema files will still have 'dict_from_yaml'
        # from the contents
        if 'contents' in dct:
            del dct['contents']
        return dct


@dataclass
class SourceFile(BaseSourceFile):
    nodes: List[str] = field(default_factory=list)
    docs: List[str] = field(default_factory=list)
    macros: List[str] = field(default_factory=list)

    @classmethod
    def big_seed(cls, path: FilePath) -> 'SourceFile':
        """Parse seeds over the size limit with just the path"""
        self = cls(path=path, checksum=FileHash.path(path.original_file_path))
        self.contents = ''
        return self

    def add_node(self, value):
        if value not in self.nodes:
            self.nodes.append(value)

    # TODO: do this a different way. This remote file kludge isn't going
    # to work long term
    @classmethod
    def remote(cls, contents: str, project_name: str) -> 'SourceFile':
        self = cls(
            path=RemoteFile(),
            checksum=FileHash.from_contents(contents),
            project_name=project_name,
            contents=contents,
        )
        return self


@dataclass
class SchemaSourceFile(BaseSourceFile):
    dfy: Dict[str, Any] = field(default_factory=dict)
    # these are in the manifest.nodes dictionary
    tests: List[str] = field(default_factory=list)
    sources: List[str] = field(default_factory=list)
    exposures: List[str] = field(default_factory=list)
    # node patches contain models, seeds, snapshots, analyses
    ndp: List[str] = field(default_factory=list)
    # any macro patches in this file by macro unique_id.
    mcp: List[str] = field(default_factory=list)
    # any source patches in this file. The entries are package, name pairs
    # Patches are only against external sources. Sources can be
    # created too, but those are in 'sources'
    sop: List[SourceKey] = field(default_factory=list)
    pp_dict: Optional[Dict[str, Any]] = None
    pp_test_index: Optional[Dict[str, Any]] = None

    @property
    def dict_from_yaml(self):
        return self.dfy

    @property
    def node_patches(self):
        return self.ndp

    @property
    def macro_patches(self):
        return self.mcp

    @property
    def source_patches(self):
        return self.sop

    def __post_serialize__(self, dct):
        dct = super().__post_serialize__(dct)
        # Remove partial parsing specific data
        for key in ('pp_files', 'pp_test_index', 'pp_dict'):
            if key in dct:
                del dct[key]
        return dct

    def append_patch(self, yaml_key, unique_id):
        self.node_patches.append(unique_id)


AnySourceFile = Union[SchemaSourceFile, SourceFile]
