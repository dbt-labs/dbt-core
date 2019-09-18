import os
from typing import List

from hologram import ValidationError

from dbt.contracts.graph.parsed import (
    IntermediateSnapshotNode, ParsedSnapshotNode
)
from dbt.exceptions import (
    CompilationException, validator_error_message
)
from dbt.node_types import NodeType
from dbt.parser.base import SQLParser
from dbt.parser.search import (
    FilesystemSearcher, BlockContents, BlockSearcher, FileBlock
)
from dbt.utils import split_path


class SnapshotParser(
    SQLParser[IntermediateSnapshotNode, ParsedSnapshotNode]
):
    def get_paths(self):
        return FilesystemSearcher(
            self.project, self.project.snapshot_paths, '.sql'
        )

    def parse_from_dict(self, dct, validate=True) -> IntermediateSnapshotNode:
        return IntermediateSnapshotNode.from_dict(dct, validate=validate)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Snapshot

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return block.path.relative_path

    def set_snapshot_attributes(self, node):
        # use the target_database setting if we got it, otherwise the
        # `database` value of the node (ultimately sourced from the `database`
        # config value), and if that is not set, use the database defined in
        # the adapter's credentials.
        if node.config.target_database:
            node.database = node.config.target_database
        elif not node.database:
            node.database = self.root_project.credentials.database

        # the target schema must be set if we got here, so overwrite the node's
        # schema
        node.schema = node.config.target_schema

        return node

    def get_fqn(self, path: str, name: str) -> List[str]:
        """Get the FQN for the node. This impacts node selection and config
        application.

        On snapshots, the fqn includes the filename.
        """
        no_ext = os.path.splitext(path)[0]
        fqn = [self.project.project_name]
        fqn.extend(split_path(no_ext))
        fqn.append(name)
        return fqn

    def transform(self, node: IntermediateSnapshotNode) -> ParsedSnapshotNode:
        try:
            parsed_node = ParsedSnapshotNode.from_dict(node.to_dict())
            self.set_snapshot_attributes(parsed_node)
            return parsed_node
        except ValidationError as exc:
            raise CompilationException(validator_error_message(exc), node)

    def parse_file(self, file_block: FileBlock) -> None:
        blocks = BlockSearcher(
            source=[file_block],
            allowed_blocks={'snapshot'},
            source_tag_factory=BlockContents,
        )
        for block in blocks:
            self.parse_node(block)
        # in case there are no snapshots declared, we still want to mark this
        # file as seen. But after we've finished, because we don't want to add
        # files with syntax errors
        self.results.get_file(file_block.file)
