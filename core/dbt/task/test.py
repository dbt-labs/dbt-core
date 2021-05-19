from agate import Table
from dataclasses import dataclass
from dbt import utils
from dbt.dataclass_schema import dbtClassMixin
import threading
from typing import Dict, Any, Optional, Union

from .compile import CompileRunner
from .run import RunTask
from .printer import print_start_line, print_test_result_line

from dbt.contracts.graph.compiled import (
    CompiledDataTestNode,
    CompiledSchemaTestNode,
    CompiledTestNode,
)
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import RunResult, TestStatus, PrimitiveDict
from dbt.context.providers import generate_runtime_model
from dbt.clients.jinja import MacroGenerator
from dbt.exceptions import (
    InternalException,
    missing_materialization
)
from dbt.graph import (
    ResourceTypeSelector,
    SelectionSpec,
    parse_test_selectors,
)
from dbt.node_types import NodeType, RunHookType
from dbt import flags


@dataclass
class TestResult(dbtClassMixin):
    validation_errors: int
    should_warn: bool
    should_error: bool
    warn_if: str
    error_if: str


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

    def before_execute(self):
        self.print_start_line()

    def execute_test(
        self,
        test: Union[CompiledDataTestNode, CompiledSchemaTestNode],
        manifest: Manifest
    ) -> TestResult:
        context = generate_runtime_model(
            test, self.config, manifest
        )

        materialization_macro = manifest.find_materialization_macro_by_name(
            self.config.project_name,
            test.get_materialization(),
            self.adapter.type()
        )

        if materialization_macro is None:
            missing_materialization(test, self.adapter.type())

        if 'config' not in context:
            raise InternalException(
                'Invalid materialization context generated, missing config: {}'
                .format(context)
            )

        # generate materialization macro
        # simple `select(*)` of the compiled test node
        macro_func = MacroGenerator(materialization_macro, context)
        # execute materialization macro
        macro_func()
        # load results from context
        # could eventually be returned directly by materialization
        result = context['load_result']('main')
        table = result['table']
        num_rows = len(table.rows)
        if num_rows != 1:
            raise InternalException(
                f"dbt internally failed to execute {test.unique_id}: "
                f"Returned {num_rows} rows, but expected "
                f"1 row"
            )
        num_cols = len(table.columns)
        if num_cols != 5:
            raise InternalException(
                f"dbt internally failed to execute {test.unique_id}: "
                f"Returned {num_cols} columns, but expected "
                f"5 columns"
            )

        test_result_data: PrimitiveDict = dict(zip(table.column_names, table.rows[0]))

        return TestResult.from_dict(test_result_data)

    def execute(self, test: CompiledTestNode, manifest: Manifest):
        result = self.execute_test(test, manifest)

        severity = test.config.severity.upper()
        thread_id = threading.current_thread().name
        num_errors = utils.pluralize(result.validation_errors, 'result')
        status = None
        message = None
        if severity == "ERROR" and result.should_error:
            status = TestStatus.Fail
            message = f'Got {num_errors}, configured to fail if {result.error_if}'
        elif result.should_warn:
            if flags.WARN_ERROR:
                status = TestStatus.Fail
                message = f'Got {num_errors}, configured to fail if {result.warn_if}'
            else:
                status = TestStatus.Warn
                message = f'Got {num_errors}, configured to warn if {result.warn_if}'
        else:
            status = TestStatus.Pass

        return RunResult(
            node=test,
            status=status,
            timing=[],
            thread_id=thread_id,
            execution_time=0,
            message=message,
            adapter_response={},
        )

    def after_execute(self, result):
        self.print_result_line(result)


class TestSelector(ResourceTypeSelector):
    def __init__(self, graph, manifest, previous_state):
        super().__init__(
            graph=graph,
            manifest=manifest,
            previous_state=previous_state,
            resource_types=[NodeType.Test],
        )


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
