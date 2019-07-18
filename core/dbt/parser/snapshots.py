
from dbt.contracts.graph.parsed import ParsedSnapshotNode, \
    IntermediateSnapshotNode
from dbt.exceptions import CompilationException, validator_error_message
from dbt.node_types import NodeType
from dbt.parser.base_sql import BaseSqlParser, SQLParseResult
import dbt.clients.jinja
import dbt.utils

from hologram import ValidationError


def set_snapshot_attributes(node):
    if node.config.target_database:
        node.database = node.config.target_database
    if node.config.target_schema:
        node.schema = node.config.target_schema

    return node


class SnapshotParser(BaseSqlParser):
    def parse_snapshots_from_file(self, file_node, tags=None):
        # the file node has a 'raw_sql' field that contains the jinja data with
        # (we hope!) `snapshot` blocks
        try:
            blocks = dbt.clients.jinja.extract_toplevel_blocks(
                file_node['raw_sql'],
                allowed_blocks={'snapshot'},
                collect_raw_data=False
            )
        except CompilationException as exc:
            if exc.node is None:
                exc.node = file_node
            raise
        for block in blocks:
            name = block.block_name
            raw_sql = block.contents
            updates = {
                'raw_sql': raw_sql,
                'name': name,
            }
            yield dbt.utils.deep_merge(file_node, updates)

    @classmethod
    def get_compiled_path(cls, name, relative_path):
        return relative_path

    @classmethod
    def get_fqn(cls, node, package_project_config, extra=[]):
        parts = dbt.utils.split_path(node.path)
        fqn = [package_project_config.project_name]
        fqn.extend(parts[:-1])
        fqn.extend(extra)
        fqn.append(node.name)

        return fqn

    def parse_from_dict(self, parsed_dict) -> IntermediateSnapshotNode:
        return IntermediateSnapshotNode.from_dict(parsed_dict)

    @staticmethod
    def validate_snapshots(node):
        if node.resource_type == NodeType.Snapshot:
            try:
                parsed_node = ParsedSnapshotNode.from_dict(node.to_dict())
                return set_snapshot_attributes(parsed_node)

            except ValidationError as exc:
                raise CompilationException(validator_error_message(exc), node)
        else:
            return node

    def parse_sql_nodes(self, nodes, tags=None):
        if tags is None:
            tags = []

        results = SQLParseResult()

        # in snapshots, we have stuff in blocks.
        for file_node in nodes:
            snapshot_nodes = list(
                self.parse_snapshots_from_file(file_node, tags=tags)
            )
            found = super().parse_sql_nodes(nodes=snapshot_nodes, tags=tags)
            # Our snapshots are all stored as IntermediateSnapshotNodes, so
            # convert them to their final form
            found.parsed = {k: self.validate_snapshots(v) for
                            k, v in found.parsed.items()}

            results.update(found)
        return results
