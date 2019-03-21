from dbt.node_runners import TestRunner
from dbt.node_types import NodeType
import dbt.ui.printer
from dbt.task.run import RunTask


class TestTask(RunTask):
    """
    Testing:
        Read schema files + custom data tests and validate that
        constraints are satisfied.
    """
    def raise_on_first_error(self):
        return False

    def before_run(self, adapter, selected_uids):
        # Don't execute on-run-* hooks for tests
        self.populate_adapter_cache(adapter)

    def after_run(self, adapter, results):
        pass

    def build_query(self):
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
        return query

    def get_runner_type(self):
        return TestRunner
