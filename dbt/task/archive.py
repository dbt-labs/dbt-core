from dbt.runner import RunManager
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from dbt.node_runners import ArchiveRunner
from dbt.node_types import NodeType

from dbt.task.base_task import RunnableTask

import dbt.ui.printer


class ArchiveTask(RunnableTask):
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

        return results
