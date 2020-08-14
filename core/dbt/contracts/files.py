import hashlib
import os
from dataclasses import dataclass, field
from typing import List, Optional, Union

from hologram import JsonSchemaMixin

from dbt.exceptions import InternalException

from .util import MacroKey, SourceKey


MAXIMUM_SEED_SIZE = 1 * 1024 * 1024
MAXIMUM_SEED_SIZE_NAME = '1MB'


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

    def seed_too_large(self) -> bool:
        """Return whether the file this represents is over the seed size limit
        """
        return os.stat(self.full_path).st_size > MAXIMUM_SEED_SIZE


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
    def from_contents(cls, contents: str, name='sha256') -> 'FileHash':
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
    def big_seed(cls, path: FilePath) -> 'SourceFile':
        """Parse seeds over the size limit with just the path"""
        self = cls(path=path, checksum=FileHash.path(path.absolute_path))
        self.contents = ''
        return self

    @classmethod
    def remote(cls, contents: str) -> 'SourceFile':
        self = cls(path=RemoteFile(), checksum=FileHash.empty())
        self.contents = contents
        return self
