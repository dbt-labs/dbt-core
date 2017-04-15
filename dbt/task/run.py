from __future__ import print_function

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.runner import RunManager
import dbt.utils

THREAD_LIMIT = 9


class RunTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        runner = RunManager(
            self.project, self.project['target-path'], self.args
        )

        results = runner.run_models(self.args.models, self.args.exclude)

        logger.info(dbt.utils.get_run_status_line(results))
