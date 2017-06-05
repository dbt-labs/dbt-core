from dbt.runner import RunManager
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from dbt.node_runners import ArchiveRunner
from dbt.utils import NodeType

import dbt.ui.printer


class ArchiveTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        runner = RunManager(
            self.project,
            self.project['target-path'],
            self.args
        )

        query = {
            'include': ['*'],
            'exclude': [],
            'resource_types': [NodeType.Archive]
        }

        results = runner.run_flat(query, ArchiveRunner)

        dbt.ui.printer.print_run_end_messages(results)
