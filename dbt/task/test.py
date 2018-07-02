from dbt.runner import RunManager
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from dbt.node_runners import TestRunner
from dbt.node_types import NodeType
import dbt.ui.printer
from dbt.task.base_task import RunnableTask


class TestTask(RunnableTask):
    """
    Testing:
        1) Create tmp views w/ 0 rows to ensure all tables, schemas, and SQL
           statements are valid
        2) Read schema files and validate that constraints are satisfied
           a) not null
           b) uniquenss
           c) referential integrity
           d) accepted value
    """
    def run(self):
        runner = RunManager(
            self.project, self.project['target-path'], self.args)

        include = self.args.models
        exclude = self.args.exclude

        query = {
            "include": self.args.models,
            "exclude": self.args.exclude,
            "resource_types": NodeType.Test
        }

        test_types = [self.args.data, self.args.schema]

        if all(test_types) or not any(test_types):
            tags = []
        elif self.args.data:
            tags = ['data']
        elif self.args.schema:
            tags = ['schema']
        else:
            raise RuntimeError("unexpected")

        query['tags'] = tags
        results = runner.run_flat(query, TestRunner)

        dbt.ui.printer.print_run_end_messages(results)

        return results
