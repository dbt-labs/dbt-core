from .run import ModelRunner, RunTask
from .printer import print_snapshot_result_line

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

    def build_query(self):
        return {
            "include": self.args.models,
            "exclude": self.args.exclude,
            "resource_types": [NodeType.Snapshot],
            "tags": [],
        }

    def get_runner_type(self):
        return SnapshotRunner
