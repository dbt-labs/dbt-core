from dbt.node_runners import ArchiveRunner
from dbt.node_types import NodeType
from dbt.task.run import RunTask


class ArchiveTask(RunTask):
    def raise_on_first_error(self):
        return False

    def build_query(self):
        return {
            'include': ['*'],
            'exclude': [],
            'resource_types': [NodeType.Archive]
        }

    def get_runner_type(self):
        return ArchiveRunner
