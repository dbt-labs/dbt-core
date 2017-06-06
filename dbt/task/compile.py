from __future__ import print_function

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.runner import RunManager
from dbt.node_runners import CompileRunner
from dbt.utils import NodeType
import dbt.ui.printer


class CompileTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        runner = RunManager(
            self.project, self.project['target-path'], self.args
        )

        query = {
            "include": self.args.models,
            "exclude": self.args.exclude,
            "resource_types": NodeType.executable(),
            "tags": set()
        }

        runner.run(query, CompileRunner)

        dbt.ui.printer.print_timestamped_line('Done.')
