from typing import Iterable, List

import jinja2

from dbt.exceptions import ParsingError
from dbt.clients import jinja
from dbt.contracts.graph.nodes import GenericTestNode, Macro
from dbt.contracts.graph.unparsed import UnparsedMacro
from dbt.contracts.files import SourceFile
from dbt.events.functions import fire_event
from dbt.events.types import GenericTestFileParse
from dbt.node_types import NodeType
from dbt.parser.base import BaseParser
from dbt.parser.search import FileBlock
from dbt.utils import MACRO_PREFIX
from dbt.flags import get_flags


class GenericTestParser(BaseParser[GenericTestNode]):
    @property
    def resource_type(self) -> NodeType:
        return NodeType.Macro

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return block.path.relative_path

    def parse_generic_test(
        self, block: jinja.BlockTag, base_node: UnparsedMacro, name: str
    ) -> Macro:
        unique_id = self.generate_unique_id(name)

        return Macro(
            path=base_node.path,
            macro_sql=block.full_block,
            original_file_path=base_node.original_file_path,
            package_name=base_node.package_name,
            resource_type=base_node.resource_type,
            name=name,
            unique_id=unique_id,
        )

    def parse_unparsed_generic_test(self, base_node: UnparsedMacro) -> Iterable[Macro]:
        try:
            blocks: List[jinja.BlockTag] = [
                t
                for t in jinja.extract_toplevel_blocks(
                    base_node.raw_code,
                    allowed_blocks={"test"},
                    collect_raw_data=False,
                )
                if isinstance(t, jinja.BlockTag)
            ]
        except ParsingError as exc:
            exc.add_node(base_node)
            raise

        for block in blocks:
            try:
                ast = jinja.parse(block.full_block)
            except ParsingError as e:
                e.add_node(base_node)
                raise

            # generic tests are structured as macros so we want to count the number of macro blocks
            generic_test_nodes = list(ast.find_all(jinja2.nodes.Macro))

            if len(generic_test_nodes) != 1:
                # things have gone disastrously wrong, we thought we only
                # parsed one block!
                raise ParsingError(
                    f"Found multiple generic tests in {block.full_block}, expected 1",
                    node=base_node,
                )

            generic_test_name = generic_test_nodes[0].name

            if not generic_test_name.startswith(MACRO_PREFIX):
                continue

            name: str = generic_test_name.replace(MACRO_PREFIX, "")
            node = self.parse_generic_test(block, base_node, name)
            yield node

    def parse_file(self, block: FileBlock):
        assert isinstance(block.file, SourceFile)
        source_file = block.file
        assert isinstance(source_file.contents, str)
        original_file_path = source_file.path.original_file_path
        if get_flags().MACRO_DEBUGGING:
            fire_event(GenericTestFileParse(path=original_file_path))

        # this is really only used for error messages
        base_node = UnparsedMacro(
            path=original_file_path,
            original_file_path=original_file_path,
            package_name=self.project.project_name,
            raw_code=source_file.contents,
            resource_type=NodeType.Macro,
            language="sql",
        )

        for node in self.parse_unparsed_generic_test(base_node):
            self.manifest.add_macro(block.file, node)
