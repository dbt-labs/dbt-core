from typing import Iterable

import jinja2.runtime

from dbt.clients.jinja import get_template
from dbt.contracts.graph.unparsed import UnparsedDocumentationFile
from dbt.contracts.graph.parsed import ParsedDocumentation
from dbt.exceptions import CompilationException, InternalException
from dbt.node_types import NodeType
from dbt.parser.base import Parser
from dbt.parser.search import (
    FullBlock, FileBlock, FilesystemSearcher, BlockSearcher
)
from dbt.utils import deep_merge, DOCS_PREFIX


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

    # TODO: could this class just render() the tag.contents() and skip this
    # whole extra module.__dict__.items() thing?
    def _parse_template_docs(self, template, docfile):
        for key, item in template.module.__dict__.items():
            if type(item) != jinja2.runtime.Macro:
                continue

            if not key.startswith(DOCS_PREFIX):
                continue

            name = key.replace(DOCS_PREFIX, '')

            unique_id = self.generate_unique_id(name)

            merged = deep_merge(
                docfile.to_dict(),
                {
                    'name': name,
                    'unique_id': unique_id,
                    'block_contents': item().strip(),
                }
            )
            yield ParsedDocumentation.from_dict(merged)

    def parse_block(self, block: FullBlock) -> Iterable[ParsedDocumentation]:
        base_node = UnparsedDocumentationFile(
            root_path=self.project.project_root,
            path=block.file.path.relative_path,
            original_file_path=block.path.original_file_path,
            package_name=self.project.project_name,
            # set contents to the actual internal contents of the block
            file_contents=block.contents,
        )
        try:
            template = get_template(block.contents, {})
        except CompilationException as e:
            e.node = base_node
            raise
        all_docs = list(self._parse_template_docs(template, base_node))
        if len(all_docs) != 1:
            raise InternalException(
                'Got {} docs in an extracted docs block: block parser '
                'mismatched with jinja'.format(len(all_docs))
            )
        return all_docs

    def parse_file(self, file_block: FileBlock):
        searcher: Iterable[FullBlock] = BlockSearcher(
            source=[file_block],
            allowed_blocks={'docs'},
            source_tag_factory=FullBlock,
        )
        for block in searcher:
            for parsed in self.parse_block(block):
                self.results.add_doc(file_block.file, parsed)
        # mark the file as seen, even if there are no macros in it
        self.results.get_file(file_block.file)
