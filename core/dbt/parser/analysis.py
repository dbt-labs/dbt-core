import os

from dbt.contracts.graph.parsed import ParsedAnalysisNode
from dbt.node_types import NodeType
from dbt.parser.base import SimpleSQLParser
from dbt.parser.search import FilesystemSearcher, FileBlock


class AnalysisParser(SimpleSQLParser[ParsedAnalysisNode]):
    def get_paths(self):
        return FilesystemSearcher(
            self.project, self.project.analysis_paths, '.sql'
        )

    def parse_from_dict(self, dct, validate=True) -> ParsedAnalysisNode:
        return ParsedAnalysisNode.from_dict(dct, validate=validate)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Analysis

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return os.path.join('analysis', block.path.relative_path)
