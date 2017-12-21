from __future__ import print_function

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.runner import RunManager
from dbt.node_types import NodeType
from dbt.node_runners import ModelRunner, OperationRunner
from dbt.exceptions import raise_compiler_error

import dbt.ui.printer

from dbt.task.base_task import RunnableTask


class RunTask(RunnableTask):
    def operation_query(self):
        operation_name = self.args.operation_name
        if "." in operation_name:
            package, operation_name = operation_name.split(".", 1)
        else:
            package = self.project['name']

        return {
            "package": package,
            "operation": operation_name
        }


    def graph_query(self):
        return {
            "include": self.args.models,
            "exclude": self.args.exclude,
            "resource_types": [NodeType.Model],
            "tags": set()
        }


    def run(self):
        runner = RunManager(
            self.project, self.project['target-path'], self.args
        )

        if hasattr(self.args, 'operation_name'):
            Runner = OperationRunner
            entrypoint  = runner.run_operation
            query = self.operation_query()
        else:
            Runner = ModelRunner
            entrypoint = runner.run
            query = self.graph_query()

        results = entrypoint(query, Runner)

        if results:
            dbt.ui.printer.print_run_end_messages(results)

        return results
