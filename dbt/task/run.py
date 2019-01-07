from __future__ import print_function

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.runner import RunManager
from dbt.node_types import NodeType
from dbt.node_runners import ModelRunner

import dbt.ui.printer

from dbt.task.base_task import RunnableTask


class RunTask(RunnableTask):
    def run(self):

        query = {
            "include": self.args.models,
            "exclude": self.args.exclude,
            "resource_types": [NodeType.Model],
            "tags": []
        }

        results = RunManager(self.config, query, ModelRunner).run()

        if results:
            dbt.ui.printer.print_run_end_messages(results)

        return results
