import os
from typing import Dict, Any

from dbt.contracts.graph.parsed import ParsedAnalysisNode, ParsedRPCNode
from dbt.parser.base_sql import BaseSqlParser


class AnalysisParser(BaseSqlParser):
    @classmethod
    def get_compiled_path(cls, name, relative_path):
        return os.path.join('analysis', relative_path)

    def parse_from_dict(
        self,
        parsed_dict: Dict[str, Any]
    ) -> ParsedAnalysisNode:
        """Given a dictionary, return the parsed entity for this parser"""
        return ParsedAnalysisNode.from_dict(parsed_dict)


class RPCCallParser(BaseSqlParser):
    def get_compiled_path(cls, name, relative_path):
        return os.path.join('rpc', relative_path)

    def parse_from_dict(self, parsed_dict: Dict[str, Any]) -> ParsedRPCNode:
        """Given a dictionary, return the parsed entity for this parser"""
        return ParsedRPCNode.from_dict(parsed_dict)
