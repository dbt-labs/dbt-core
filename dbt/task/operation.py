from dbt.node_runners import OperationRunner
from dbt.node_types import NodeType
from dbt.runner import RunManager
from dbt.task.base_task import RunnableTask
import dbt.ui.printer


class OperationTask(RunnableTask):
    def run(self):
        runner = RunManager(
            self.project,
            self.project["target-path"],
            self.args,
        )
        query = {
            "include": ["*"],
            "exclude": [],
            "resource_types": [NodeType.Operation],
        }
        results = runner.run_flat(query, OperationRunner)

        dbt.ui.printer.print_run_end_messages(results)
        return results
