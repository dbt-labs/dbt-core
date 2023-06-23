from typing import Iterable, Optional

import re

from dbt.clients.jinja import get_rendered
from dbt.contracts.files import SourceFile
from dbt.contracts.graph.nodes import Documentation
from dbt.node_types import NodeType
from dbt.parser.base import Parser
from dbt.parser.search import BlockContents, FileBlock, BlockSearcher


SHOULD_PARSE_RE = re.compile(r"{[{%]")


class DocumentationParser(Parser[Documentation]):
    @property
    def resource_type(self) -> NodeType:
        return NodeType.Documentation

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return block.path.relative_path

    def generate_unique_id(self, resource_name: str, _: Optional[str] = None) -> str:
        # For consistency, use the same format for doc unique_ids
        return f"doc.{self.project.project_name}.{resource_name}"

    def parse_block(self, block: BlockContents) -> Iterable[Documentation]:
        unique_id = self.generate_unique_id(block.name)
        contents = get_rendered(block.contents, {}).strip()

        doc = Documentation(
            path=block.file.path.relative_path,
            original_file_path=block.path.original_file_path,
            package_name=self.project.project_name,
            unique_id=unique_id,
            name=block.name,
            block_contents=contents,
            resource_type=NodeType.Documentation,
        )
        return [doc]

    def parse_file(self, file_block: FileBlock):
        assert isinstance(file_block.file, SourceFile)
        searcher: Iterable[BlockContents] = BlockSearcher(
            source=[file_block],
            allowed_blocks={"docs"},
            source_tag_factory=BlockContents,
        )
        for block in searcher:
            for parsed in self.parse_block(block):
                self.manifest.add_doc(file_block.file, parsed)
