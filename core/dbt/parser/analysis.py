import os

from dbt.contracts.graph.nodes import AnalysisNode
from dbt.node_types import NodeType
from dbt.parser.base import SimpleSQLParser
from dbt.parser.search import FileBlock


class AnalysisParser(SimpleSQLParser[AnalysisNode]):
    def parse_from_dict(self, dct, validate=True) -> AnalysisNode:
        if validate:
            AnalysisNode.validate(dct)
        return AnalysisNode.from_dict(dct)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Analysis

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return os.path.join("analysis", block.path.relative_path)
