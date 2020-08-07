import os
from dataclasses import dataclass
from typing import (
    List, Callable, Iterable, Set, Union, Iterator, TypeVar, Generic
)

from dbt.clients.jinja import extract_toplevel_blocks, BlockTag
from dbt.clients.system import find_matching
from dbt.config import Project
from dbt.contracts.files import SourceFile, FilePath
from dbt.exceptions import CompilationException, InternalException


@dataclass
class FileBlock:
    file: SourceFile

    @property
    def name(self):
        base = os.path.basename(self.file.path.relative_path)
        name, _ = os.path.splitext(base)
        return name

    @property
    def contents(self):
        return self.file.contents

    @property
    def path(self):
        return self.file.path


@dataclass
class BlockContents(FileBlock):
    file: SourceFile  # if you remove this, mypy will get upset
    block: BlockTag

    @property
    def name(self):
        return self.block.block_name

    @property
    def contents(self):
        return self.block.contents


@dataclass
class FullBlock(FileBlock):
    file: SourceFile  # if you remove this, mypy will get upset
    block: BlockTag

    @property
    def name(self):
        return self.block.block_name

    @property
    def contents(self):
        return self.block.full_block


class FilesystemSearcher(Iterable[FilePath]):
    def __init__(
        self, project: Project, relative_dirs: List[str], extension: str
    ) -> None:
        self.project = project
        self.relative_dirs = relative_dirs
        self.extension = extension

    def __iter__(self) -> Iterator[FilePath]:
        ext = "[!.#~]*" + self.extension

        root = self.project.project_root

        for result in find_matching(root, self.relative_dirs, ext):
            if 'searched_path' not in result or 'relative_path' not in result:
                raise InternalException(
                    'Invalid result from find_matching: {}'.format(result)
                )
            file_match = FilePath(
                searched_path=result['searched_path'],
                relative_path=result['relative_path'],
                project_root=root,
            )
            yield file_match


Block = Union[BlockContents, FullBlock]

BlockSearchResult = TypeVar('BlockSearchResult', BlockContents, FullBlock)

BlockSearchResultFactory = Callable[[SourceFile, BlockTag], BlockSearchResult]


class BlockSearcher(Generic[BlockSearchResult], Iterable[BlockSearchResult]):
    def __init__(
        self,
        source: List[FileBlock],
        allowed_blocks: Set[str],
        source_tag_factory: BlockSearchResultFactory
    ) -> None:
        self.source = source
        self.allowed_blocks = allowed_blocks
        self.source_tag_factory: BlockSearchResultFactory = source_tag_factory

    def extract_blocks(self, source_file: FileBlock) -> Iterable[BlockTag]:
        try:
            blocks = extract_toplevel_blocks(
                source_file.contents,
                allowed_blocks=self.allowed_blocks,
                collect_raw_data=False
            )
            # this makes mypy happy, and this is an invariant we really need
            for block in blocks:
                assert isinstance(block, BlockTag)
                yield block

        except CompilationException as exc:
            if exc.node is None:
                exc.add_node(source_file)
            raise

    def __iter__(self) -> Iterator[BlockSearchResult]:
        for entry in self.source:
            for block in self.extract_blocks(entry):
                yield self.source_tag_factory(entry.file, block)
