from dbt.context.context_config import ContextConfigType
from dbt.contracts.files import SourceFile, FilePath
from dbt.contracts.graph.parsed import ParsedSeedNode
from dbt.node_types import NodeType
from dbt.parser.base import SimpleSQLParser
from dbt.parser.search import FileBlock, FilesystemSearcher


class SeedParser(SimpleSQLParser[ParsedSeedNode]):
    def get_paths(self):
        return FilesystemSearcher(
            self.project, self.project.data_paths, '.csv'
        )

    def parse_from_dict(self, dct, validate=True) -> ParsedSeedNode:
        return ParsedSeedNode.from_dict(dct, validate=validate)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Seed

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return block.path.relative_path

    def render_with_context(
        self, parsed_node: ParsedSeedNode, config: ContextConfigType
    ) -> None:
        """Seeds don't need to do any rendering."""

    def load_file(
        self, match: FilePath, *, set_contents: bool = False
    ) -> SourceFile:
        if match.seed_too_large():
            # We don't want to calculate a hash of this file. Use the path.
            return SourceFile.big_seed(match)
        else:
            # We want to calculate a hash, but we don't need the contents
            return super().load_file(match, set_contents=set_contents)
