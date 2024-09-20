import os
from dataclasses import dataclass
from typing import Iterable

from dbt.contracts.graph.manifest import SourceFile
from dbt.contracts.graph.nodes import Macro, SqlNode
from dbt.contracts.graph.unparsed import UnparsedMacro
from dbt.node_types import NodeType
from dbt.parser.base import SimpleSQLParser
from dbt.parser.macros import MacroParser
from dbt.parser.search import FileBlock
from dbt_common.exceptions import DbtInternalError


@dataclass
class SqlBlock(FileBlock):
    block_name: str

    @property
    def name(self):
        return self.block_name


class SqlBlockParser(SimpleSQLParser[SqlNode]):
    def parse_from_dict(self, dct, validate=True) -> SqlNode:
        if validate:
            SqlNode.validate(dct)
        return SqlNode.from_dict(dct)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.SqlOperation

    @staticmethod
    def get_compiled_path(block: FileBlock):
        # we do it this way to make mypy happy
        if not isinstance(block, SqlBlock):
            raise DbtInternalError(
                "While parsing SQL operation, got an actual file block instead of "
                "an SQL block: {}".format(block)
            )

        return os.path.join("sql", block.name)

    def parse_remote(self, sql: str, name: str) -> SqlNode:
        source_file = SourceFile.remote(sql, self.project.project_name, "sql")
        contents = SqlBlock(block_name=name, file=source_file)
        return self.parse_node(contents)


class SqlMacroParser(MacroParser):
    def parse_remote(self, contents) -> Iterable[Macro]:
        base = UnparsedMacro(
            path="from remote system",
            original_file_path="from remote system",
            package_name=self.project.project_name,
            raw_code=contents,
            language="sql",
            resource_type=NodeType.Macro,
        )
        for node in self.parse_unparsed_macros(base):
            yield node
