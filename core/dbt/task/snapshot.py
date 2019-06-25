from dbt.node_runners import SnapshotRunner
from dbt.node_types import NodeType
from dbt.task.run import RunTask
from dbt.deprecations import warn


class SnapshotTask(RunTask):
    def __init__(self, args, config):
        super().__init__(args, config)
        if args.which == 'archive':
            warn('archives')

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
