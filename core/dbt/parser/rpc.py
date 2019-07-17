import os
from dataclasses import dataclass
from typing import Iterable

from dbt.contracts.graph.manifest import SourceFile
from dbt.contracts.graph.parsed import ParsedRPCNode, ParsedMacro
from dbt.contracts.graph.unparsed import UnparsedMacro
from dbt.node_types import NodeType, MacroType
from dbt.parser.base import SimpleSQLParser
from dbt.parser.macros import MacroParser
from dbt.parser.search import FileBlock


@dataclass
class RPCBlock(FileBlock):
    rpc_name: str

    @property
    def name(self):
        return self.rpc_name


class RPCCallParser(SimpleSQLParser[ParsedRPCNode]):
    def get_paths(self):
        return []

    def parse_from_dict(self, dct, validate=True) -> ParsedRPCNode:
        return ParsedRPCNode.from_dict(dct, validate=validate)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.RPCCall

    def get_compiled_path(cls, block: RPCBlock):
        return os.path.join('rpc', block.name)

    def parse_remote(self, sql: str, name: str) -> ParsedRPCNode:
        source_file = SourceFile.remote(contents=sql)
        contents = RPCBlock(rpc_name=name, file=source_file)
        return self.parse_node(contents)


class RPCMacroParser(MacroParser):
    def parse_remote(self, contents) -> Iterable[ParsedMacro]:
        base = UnparsedMacro(
            path='from remote system',
            original_file_path='from remote system',
            package_name=self.project.project_name,
            raw_sql=contents,
            root_path=self.project.project_root,
            resource_type=MacroType(NodeType.Macro),
        )
        for node in self.parse_unparsed_macros(base):
            yield node
