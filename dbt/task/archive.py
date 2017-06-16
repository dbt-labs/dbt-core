from dbt.runner import RunManager
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from dbt.node_runners import ArchiveRunner
from dbt.utils import NodeType

from dbt.task.base_task import BaseTask

import dbt.ui.printer


class ArchiveTask(BaseTask):
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

    def interpret_results(self, results):

