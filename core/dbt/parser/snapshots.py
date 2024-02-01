import os
from typing import List

from dbt_common.dataclass_schema import ValidationError

from dbt.contracts.graph.nodes import IntermediateSnapshotNode, SnapshotNode
from dbt.exceptions import SnapshopConfigError
from dbt.node_types import NodeType
from dbt.parser.base import SQLParser
from dbt.parser.search import BlockContents, BlockSearcher, FileBlock
from dbt.utils import split_path


class SnapshotParser(SQLParser[IntermediateSnapshotNode, SnapshotNode]):
    def parse_from_dict(self, dct, validate=True) -> IntermediateSnapshotNode:
        if validate:
            IntermediateSnapshotNode.validate(dct)
        return IntermediateSnapshotNode.from_dict(dct)

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
        # We need to set relation_name again, since database/schema might have changed
        self._update_node_relation_name(node)

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

    def transform(self, node: IntermediateSnapshotNode) -> SnapshotNode:
        try:
            # The config_call_dict is not serialized, because normally
            # it is not needed after parsing. But since the snapshot node
            # does this extra to_dict, save and restore it, to keep
            # the model config when there is also schema config.
            config_call_dict = node.config_call_dict
            dct = node.to_dict(omit_none=True)
            parsed_node = SnapshotNode.from_dict(dct)
            parsed_node.config_call_dict = config_call_dict
            self.set_snapshot_attributes(parsed_node)
            return parsed_node
        except ValidationError as exc:
            raise SnapshopConfigError(exc, node)

    def parse_file(self, file_block: FileBlock) -> None:
        blocks = BlockSearcher(
            source=[file_block],
            allowed_blocks={"snapshot"},
            source_tag_factory=BlockContents,
        )
        for block in blocks:
            self.parse_node(block)
