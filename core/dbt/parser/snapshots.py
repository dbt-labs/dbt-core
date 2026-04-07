import os
from typing import List

from dbt.contracts.graph.nodes import SnapshotNode
from dbt.node_types import NodeType
from dbt.parser.base import SQLParser
from dbt.parser.search import BlockContents, BlockSearcher, FileBlock
from dbt.utils import split_path


class SnapshotParser(SQLParser[SnapshotNode]):
    def parse_from_dict(self, dct, validate=True) -> SnapshotNode:
        if validate:
            SnapshotNode.validate(dct)
        return SnapshotNode.from_dict(dct)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Snapshot

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return block.path.relative_path

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

    def parse_file(self, file_block: FileBlock) -> None:
        blocks = BlockSearcher(
            source=[file_block],
            allowed_blocks={"snapshot"},
            source_tag_factory=BlockContents,
        )
        for block in blocks:
            self.parse_node(block)
