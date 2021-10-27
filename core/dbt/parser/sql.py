import os
from dataclasses import dataclass
from typing import Iterable

from dbt.contracts.graph.manifest import SourceFile
from dbt.contracts.graph.parsed import ParsedSqlNode, ParsedMacro
from dbt.contracts.graph.unparsed import UnparsedMacro
from dbt.exceptions import InternalException
from dbt.node_types import NodeType
from dbt.parser.base import SimpleSQLParser
from dbt.parser.macros import MacroParser
from dbt.parser.search import FileBlock


@dataclass
class SqlBlock(FileBlock):
    block_name: str

    @property
    def name(self):
        return self.block_name


class SqlBlockParser(SimpleSQLParser[ParsedSqlNode]):
    def parse_from_dict(self, dct, validate=True) -> ParsedSqlNode:
        if validate:
            ParsedSqlNode.validate(dct)
        return ParsedSqlNode.from_dict(dct)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.SqlOperation

    @staticmethod
    def get_compiled_path(block: FileBlock):
        # we do it this way to make mypy happy
        if not isinstance(block, SqlBlock):
            raise InternalException(
                'While parsing SQL operation, got an actual file block instead of '
                'an SQL block: {}'.format(block)
            )

        return os.path.join('sql', block.name)

    def parse_remote(self, sql: str, name: str) -> ParsedSqlNode:
        source_file = SourceFile.remote(sql, self.project.project_name)
        contents = SqlBlock(block_name=name, file=source_file)
        return self.parse_node(contents)


class SqlMacroParser(MacroParser):
    def parse_remote(self, contents) -> Iterable[ParsedMacro]:
        base = UnparsedMacro(
            path='from remote system',
            original_file_path='from remote system',
            package_name=self.project.project_name,
            raw_sql=contents,
            root_path=self.project.project_root,
            resource_type=NodeType.Macro,
        )
        for node in self.parse_unparsed_macros(base):
            yield node
