from .run import ModelRunner, RunTask
from .printer import print_snapshot_result_line

from dbt.exceptions import InternalException
from dbt.graph import ResourceTypeSelector
from dbt.node_types import NodeType


class SnapshotRunner(ModelRunner):
    def describe_node(self):
        return "snapshot {}".format(self.get_node_representation())

    def print_result_line(self, result):
        print_snapshot_result_line(
            result,
            self.get_node_representation(),
            self.node_index,
            self.num_nodes)


class SnapshotTask(RunTask):
    def raise_on_first_error(self):
        return False

    def defer_to_manifest(self, adapter, selected_uids):
        # snapshots don't defer
        return

    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise InternalException(
                'manifest and graph must be set to get perform node selection'
            )
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[NodeType.Snapshot],
        )

    def get_runner_type(self, _):
        return SnapshotRunner
