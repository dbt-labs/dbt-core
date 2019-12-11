from dbt.contracts.graph.parsed import ParsedModelNode
from dbt.node_types import NodeType
from dbt.parser.base import SimpleSQLParser
from dbt.parser.search import FilesystemSearcher, FileBlock


class ModelParser(SimpleSQLParser[ParsedModelNode]):
    def get_paths(self):
        return FilesystemSearcher(
            self.project, self.project.source_paths, '.sql'
        )

    def parse_from_dict(self, dct, validate=True) -> ParsedModelNode:
        return ParsedModelNode.from_dict(dct, validate=validate)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Model

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return block.path.relative_path
