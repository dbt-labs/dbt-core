from typing import Iterable, List

import jinja2

from dbt.clients import jinja
from dbt.contracts.graph.unparsed import UnparsedMacro
from dbt.contracts.graph.parsed import ParsedMacro
from dbt.contracts.files import FilePath, SourceFile
from dbt.exceptions import CompilationException
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType
from dbt.parser.base import BaseParser
from dbt.parser.search import FileBlock, FilesystemSearcher
from dbt.utils import MACRO_PREFIX


class MacroParser(BaseParser[ParsedMacro]):
    # This is only used when creating a MacroManifest separate
    # from the normal parsing flow.
    def get_paths(self) -> List[FilePath]:
        return list(FilesystemSearcher(
            project=self.project,
            relative_dirs=self.project.macro_paths,
            extension='.sql',
        ))

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Macro

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return block.path.relative_path

    def parse_macro(
        self, block: jinja.BlockTag, base_node: UnparsedMacro, name: str
    ) -> ParsedMacro:
        unique_id = self.generate_unique_id(name)

        return ParsedMacro(
            path=base_node.path,
            macro_sql=block.full_block,
            original_file_path=base_node.original_file_path,
            package_name=base_node.package_name,
            root_path=base_node.root_path,
            resource_type=base_node.resource_type,
            name=name,
            unique_id=unique_id,
        )

    def parse_unparsed_macros(
        self, base_node: UnparsedMacro
    ) -> Iterable[ParsedMacro]:
        try:
            blocks: List[jinja.BlockTag] = [
                t for t in
                jinja.extract_toplevel_blocks(
                    base_node.raw_sql,
                    allowed_blocks={'macro', 'materialization', 'test'},
                    collect_raw_data=False,
                )
                if isinstance(t, jinja.BlockTag)
            ]
        except CompilationException as exc:
            exc.add_node(base_node)
            raise

        for block in blocks:
            try:
                ast = jinja.parse(block.full_block)
            except CompilationException as e:
                e.add_node(base_node)
                raise

            macro_nodes = list(ast.find_all(jinja2.nodes.Macro))

            if len(macro_nodes) != 1:
                # things have gone disastrously wrong, we thought we only
                # parsed one block!
                raise CompilationException(
                    f'Found multiple macros in {block.full_block}, expected 1',
                    node=base_node
                )

            macro_name = macro_nodes[0].name

            if not macro_name.startswith(MACRO_PREFIX):
                continue

            name: str = macro_name.replace(MACRO_PREFIX, '')
            node = self.parse_macro(block, base_node, name)
            yield node

    def parse_file(self, block: FileBlock):
        assert isinstance(block.file, SourceFile)
        source_file = block.file
        assert isinstance(source_file.contents, str)
        original_file_path = source_file.path.original_file_path
        logger.debug("Parsing {}".format(original_file_path))

        # this is really only used for error messages
        base_node = UnparsedMacro(
            path=original_file_path,
            original_file_path=original_file_path,
            package_name=self.project.project_name,
            raw_sql=source_file.contents,
            root_path=self.project.project_root,
            resource_type=NodeType.Macro,
        )

        for node in self.parse_unparsed_macros(base_node):
            self.manifest.add_macro(block.file, node)
