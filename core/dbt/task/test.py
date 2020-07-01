from typing import Dict, Any

from .compile import CompileRunner
from .run import RunTask
from .printer import print_start_line, print_test_result_line

from dbt.contracts.graph.compiled import (
    CompiledDataTestNode,
    CompiledSchemaTestNode,
    CompiledTestNode,
)
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import RunModelResult
from dbt.exceptions import raise_compiler_error, InternalException
from dbt import flags
from dbt.node_types import NodeType, RunHookType


class TestRunner(CompileRunner):
    def describe_node(self):
        node_name = self.node.name
        return "test {}".format(node_name)

    def print_result_line(self, result):
        schema_name = self.node.schema
        print_test_result_line(result, schema_name, self.node_index,
                               self.num_nodes)

    def print_start_line(self):
        description = self.describe_node()
        print_start_line(description, self.node_index, self.num_nodes)

    def execute_data_test(self, test: CompiledDataTestNode):
        sql = (
            f'select count(*) as errors from (\n{test.injected_sql}\n) sbq'
        )
        res, table = self.adapter.execute(sql, auto_begin=True, fetch=True)

        num_rows = len(table.rows)
        if num_rows != 1:
            num_cols = len(table.columns)
            # since we just wrapped our query in `select count(*)`, we are in
            # big trouble!
            raise InternalException(
                f"dbt internally failed to execute {test.unique_id}: "
                f"Returned {num_rows} rows and {num_cols} cols, but expected "
                f"1 row and 1 column"
            )
        return table[0][0]

    def execute_schema_test(self, test: CompiledSchemaTestNode):
        res, table = self.adapter.execute(
            test.injected_sql,
            auto_begin=True,
            fetch=True,
        )

        num_rows = len(table.rows)
        if num_rows != 1:
            num_cols = len(table.columns)
            raise_compiler_error(
                f"Bad test {test.test_metadata.name}: "
                f"Returned {num_rows} rows and {num_cols} cols, but expected "
                f"1 row and 1 column"
            )
        return table[0][0]

    def before_execute(self):
        self.print_start_line()

    def execute(self, test: CompiledTestNode, manifest: Manifest):
        if isinstance(test, CompiledDataTestNode):
            failed_rows = self.execute_data_test(test)
        elif isinstance(test, CompiledSchemaTestNode):
            failed_rows = self.execute_schema_test(test)
        else:

            raise InternalException(
                f'Expected compiled schema test or compiled data test, got '
                f'{type(test)}'
            )
        severity = test.config.severity.upper()

        if failed_rows == 0:
            return RunModelResult(test, status=failed_rows)
        elif severity == 'ERROR' or flags.WARN_ERROR:
            return RunModelResult(test, status=failed_rows, fail=True)
        else:
            return RunModelResult(test, status=failed_rows, warn=True)

    def after_execute(self, result):
        self.print_result_line(result)


class TestTask(RunTask):
    """
    Testing:
        Read schema files + custom data tests and validate that
        constraints are satisfied.
    """
    def raise_on_first_error(self):
        return False

    def safe_run_hooks(
        self, adapter, hook_type: RunHookType, extra_context: Dict[str, Any]
    ) -> None:
        # Don't execute on-run-* hooks for tests
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
