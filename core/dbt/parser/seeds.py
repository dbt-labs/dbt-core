from dbt.context.context_config import ContextConfig
from dbt.contracts.graph.nodes import SeedNode
from dbt.node_types import NodeType
from dbt.parser.base import SimpleSQLParser
from dbt.parser.search import FileBlock


class SeedParser(SimpleSQLParser[SeedNode]):
    def parse_from_dict(self, dct, validate=True) -> SeedNode:
        # seeds need the root_path because the contents are not loaded
        dct["root_path"] = self.project.project_root
        if "language" in dct:
            del dct["language"]
        # raw_code is not currently used, but it might be in the future
        if validate:
            SeedNode.validate(dct)
        return SeedNode.from_dict(dct)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Seed

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return block.path.relative_path

    def render_with_context(self, parsed_node: SeedNode, config: ContextConfig) -> None:
        """Seeds don't need to do any rendering."""
