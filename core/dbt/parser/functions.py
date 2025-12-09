from dbt.artifacts.resources.types import NodeType
from dbt.contracts.files import SourceFile
from dbt.contracts.graph.nodes import FunctionNode, ManifestNode
from dbt.parser.base import SimpleParser
from dbt.parser.search import FileBlock


class FunctionParser(SimpleParser[FileBlock, FunctionNode]):
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

    # overrides SimpleSQLParser.add_result_node
    def add_result_node(self, block: FileBlock, node: ManifestNode):
        assert isinstance(node, FunctionNode), "Got non FunctionNode in FunctionParser"
        file = block.file
        assert isinstance(file, SourceFile)
        if node.config.enabled:
            self.manifest.add_function(file, node)
        else:
            self.manifest.add_disabled(file, node)

    def parse_file(self, file_block: FileBlock) -> None:
        self.parse_node(file_block)
