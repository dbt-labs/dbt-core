from typing import Dict, Any

from dbt.contracts.graph.parsed import ParsedModelNode
from dbt.parser.base_sql import BaseSqlParser


class ModelParser(BaseSqlParser):
    @classmethod
    def get_compiled_path(cls, name, relative_path):
        return relative_path

    def parse_from_dict(self, parsed_dict: Dict[str, Any]) -> ParsedModelNode:
        """Given a dictionary, return the parsed entity for this parser"""
        return ParsedModelNode.from_dict(parsed_dict)
