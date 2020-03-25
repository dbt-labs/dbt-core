from dbt.contracts.graph.parsed import ParsedDataTestNode
from dbt.node_types import NodeType
from dbt.parser.base import SimpleSQLParser
from dbt.parser.search import FilesystemSearcher, FileBlock
from dbt.utils import get_pseudo_test_path


class DataTestParser(SimpleSQLParser[ParsedDataTestNode]):
    def get_paths(self):
        return FilesystemSearcher(
            self.project, self.project.test_paths, '.sql'
        )

    def parse_from_dict(self, dct, validate=True) -> ParsedDataTestNode:
        return ParsedDataTestNode.from_dict(dct, validate=validate)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Test

    def transform(self, node):
        if 'data' not in node.tags:
            node.tags.append('data')
        return node

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return get_pseudo_test_path(block.name, block.path.relative_path,
                                    'data_test')
