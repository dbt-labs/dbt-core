from dbt.context.context_config import ContextConfig
from dbt.contracts.graph.parsed import ParsedSeedNode
from dbt.node_types import NodeType
from dbt.parser.base import SimpleSQLParser
from dbt.parser.search import FileBlock


class SeedParser(SimpleSQLParser[ParsedSeedNode]):
    def parse_from_dict(self, dct, validate=True) -> ParsedSeedNode:
        if validate:
            ParsedSeedNode.validate(dct)
        return ParsedSeedNode.from_dict(dct)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Seed

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return block.path.relative_path

    def render_with_context(self, parsed_node: ParsedSeedNode, config: ContextConfig) -> None:
        """Seeds don't need to do any rendering."""
