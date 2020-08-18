from typing import Dict, Any, Set

from .compile import CompileRunner
from .run import RunTask
from .printer import print_start_line, print_test_result_line

from dbt.contracts.graph.compiled import (
    CompiledDataTestNode,
    CompiledSchemaTestNode,
    CompiledTestNode,
)
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import (
    ParsedDataTestNode,
    ParsedSchemaTestNode,
)
from dbt.contracts.results import RunModelResult
from dbt.exceptions import raise_compiler_error, InternalException
from dbt.graph import (
    ResourceTypeSelector,
    SelectionSpec,
    UniqueId,
    parse_test_selectors,
)
from dbt.node_types import NodeType, RunHookType
from dbt import flags


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
        res, table = self.adapter.execute(
            test.injected_sql, auto_begin=True, fetch=True
        )

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


DATA_TEST_TYPES = (CompiledDataTestNode, ParsedDataTestNode)
SCHEMA_TEST_TYPES = (CompiledSchemaTestNode, ParsedSchemaTestNode)


class TestSelector(ResourceTypeSelector):
    def __init__(self, graph, manifest, previous_state):
        super().__init__(
            graph=graph,
            manifest=manifest,
            previous_state=previous_state,
            resource_types=[NodeType.Test],
        )

    def expand_selection(self, selected: Set[UniqueId]) -> Set[UniqueId]:
        selected_tests = {
            n for n in self.graph.select_successors(selected)
            if self.manifest.nodes[n].resource_type == NodeType.Test
        }
        return selected | selected_tests


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

    def get_selection_spec(self) -> SelectionSpec:
        base_spec = super().get_selection_spec()
        return parse_test_selectors(
            data=self.args.data,
            schema=self.args.schema,
            base=base_spec
        )

    def get_node_selector(self) -> TestSelector:
        if self.manifest is None or self.graph is None:
            raise InternalException(
                'manifest and graph must be set to get perform node selection'
            )
        return TestSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
        )

    def get_runner_type(self):
        return TestRunner
