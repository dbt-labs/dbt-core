import os
from typing import List

from dbt.context.context_config import ContextConfig
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
        return block.name + ".sql"

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

    def parse_node(self, block: FileBlock) -> SnapshotNode:
        compiled_path: str = self.get_compiled_path(block)
        # Use the file's relative_path for FQN computation (not the compiled_path)
        # to maintain backward compatibility. The compiled_path is based on the
        # snapshot name for uniqueness (multiple snapshots can share one file),
        # but the FQN should still reflect the file structure.
        fqn = self.get_fqn(block.path.relative_path, block.name)

        config: ContextConfig = self.initial_config(fqn)

        node = self._create_parsetime_node(
            block=block,
            path=compiled_path,
            config=config,
            fqn=fqn,
        )
        self.render_update(node, config)
        self.add_result_node(block, node)
        return node

    def parse_file(self, file_block: FileBlock) -> None:
        blocks = BlockSearcher(
            source=[file_block],
            allowed_blocks={"snapshot"},
            source_tag_factory=BlockContents,
        )
        for block in blocks:
            self.parse_node(block)
