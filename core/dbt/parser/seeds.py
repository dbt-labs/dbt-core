from dbt.contracts.graph.manifest import SourceFile, FilePath
from dbt.contracts.graph.parsed import ParsedSeedNode
from dbt.node_types import NodeType
from dbt.source_config import SourceConfig
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
        self, parsed_node: ParsedSeedNode, config: SourceConfig
    ) -> None:
        """Seeds don't need to do any rendering."""

    def load_file(self, match: FilePath) -> SourceFile:
        return SourceFile.seed(match)

    def _create_parsetime_node(
        self,
        block: FileBlock,
        path: str,
        config: SourceConfig,
        name=None,
        **kwargs,
    ) -> ParsedSeedNode:
        return super()._create_parsetime_node(
            block=block,
            path=path,
            config=config,
            name=name,
            seed_file_path=block.path.full_path,
        )
