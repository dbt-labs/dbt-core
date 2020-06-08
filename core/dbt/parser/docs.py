from typing import Iterable

import re

from dbt.clients.jinja import get_rendered
from dbt.contracts.graph.parsed import ParsedDocumentation
from dbt.node_types import NodeType
from dbt.parser.base import Parser
from dbt.parser.search import (
    BlockContents, FileBlock, FilesystemSearcher, BlockSearcher
)


SHOULD_PARSE_RE = re.compile(r'{[{%]')


class DocumentationParser(Parser[ParsedDocumentation]):
    def get_paths(self):
        return FilesystemSearcher(
            project=self.project,
            relative_dirs=self.project.docs_paths,
            extension='.md',
        )

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Documentation

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return block.path.relative_path

    def generate_unique_id(self, resource_name: str) -> str:
        # because docs are in their own graph namespace, node type doesn't
        # need to be part of the unique ID.
        return '{}.{}'.format(self.project.project_name, resource_name)

    def parse_block(
        self, block: BlockContents
    ) -> Iterable[ParsedDocumentation]:
        unique_id = self.generate_unique_id(block.name)
        contents = get_rendered(block.contents, {}).strip()

        doc = ParsedDocumentation(
            root_path=self.project.project_root,
            path=block.file.path.relative_path,
            original_file_path=block.path.original_file_path,
            package_name=self.project.project_name,
            unique_id=unique_id,
            name=block.name,
            block_contents=contents,
        )
        return [doc]

    def parse_file(self, file_block: FileBlock):
        searcher: Iterable[BlockContents] = BlockSearcher(
            source=[file_block],
            allowed_blocks={'docs'},
            source_tag_factory=BlockContents,
        )
        for block in searcher:
            for parsed in self.parse_block(block):
                self.results.add_doc(file_block.file, parsed)
        # mark the file as seen, even if there are no macros in it
        self.results.get_file(file_block.file)
