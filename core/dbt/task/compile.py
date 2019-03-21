from dbt.node_runners import CompileRunner
from dbt.node_types import NodeType
import dbt.ui.printer

from dbt.task.runnable import RunnableTask


class CompileTask(RunnableTask):
    def raise_on_first_error(self):
        return True

    def build_query(self):
        return {
            "include": self.args.models,
            "exclude": self.args.exclude,
            "resource_types": NodeType.executable(),
            "tags": [],
        }

    def get_runner_type(self):
        return CompileRunner

    def task_end_messages(self, results):
        dbt.ui.printer.print_timestamped_line('Done.')
