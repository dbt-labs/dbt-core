from dbt.runner import RunManager
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
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

        results = runner.run_archives(['*'], [])

        dbt.ui.printer.print_run_end_messages(results)
