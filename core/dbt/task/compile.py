from __future__ import print_function

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.runner import RunManager
from dbt.node_runners import CompileRunner
from dbt.node_types import NodeType
import dbt.ui.printer

from dbt.task.base_task import RunnableTask


class CompileTask(RunnableTask):
    def run(self):

        query = {
            "include": self.args.models,
            "exclude": self.args.exclude,
            "resource_types": NodeType.executable(),
            "tags": [],
        }
        results = RunManager(self.config, query, CompileRunner).run()

        dbt.ui.printer.print_timestamped_line('Done.')

        return results
