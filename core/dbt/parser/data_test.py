from typing import Dict, Any

from dbt.contracts.graph.parsed import ParsedTestNode
from dbt.parser.base_sql import BaseSqlParser
import dbt.utils


class DataTestParser(BaseSqlParser):
    @classmethod
    def get_compiled_path(cls, name, relative_path):
        return dbt.utils.get_pseudo_test_path(name, relative_path, 'data_test')

    def parse_from_dict(self, parsed_dict: Dict[str, Any]) -> ParsedTestNode:
        """Given a dictionary, return the parsed entity for this parser"""
        return ParsedTestNode.from_dict(parsed_dict)
