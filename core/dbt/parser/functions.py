from dbt.artifacts.resources.types import NodeType
from dbt.contracts.graph.nodes import FunctionNode
from dbt.parser.base import SimpleSQLParser
from dbt.parser.search import FileBlock


class FunctionParser(SimpleSQLParser[FunctionNode]):
    def parse_from_dict(self, dct, validate=True) -> FunctionNode:
        if validate:
            FunctionNode.validate(dct)
        return FunctionNode.from_dict(dct)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Function

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return block.path.relative_path
