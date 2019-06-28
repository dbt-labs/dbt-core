from dbt.node_runners import SnapshotRunner
from dbt.node_types import NodeType
from dbt.task.run import RunTask


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
