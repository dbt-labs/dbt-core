import threading
from typing import Dict, Any, Union

from .compile import CompileRunner
from .run import RunTask
from .printer import print_start_line, print_test_result_line

from dbt.contracts.graph.compiled import (
    CompiledDataTestNode,
    CompiledSchemaTestNode,
    CompiledTestNode,
)
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import RunResult, TestStatus
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
    ) -> int:
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
            num_cols = len(table.columns)
            # since we just wrapped our query in `select count(*)`, we are in
            # big trouble!
            raise InternalException(
                f"dbt internally failed to execute {test.unique_id}: "
                f"Returned {num_rows} rows and {num_cols} cols, but expected "
                f"1 row and 1 column"
            )
        return int(table[0][0])

    def execute(self, test: CompiledTestNode, manifest: Manifest):
        failed_rows = self.execute_test(test, manifest)

        severity = test.config.severity.upper()
        thread_id = threading.current_thread().name
        status = None
        if failed_rows == 0:
            status = TestStatus.Pass
        elif severity == 'ERROR' or flags.WARN_ERROR:
            status = TestStatus.Fail
        else:
            status = TestStatus.Warn

        return RunResult(
            node=test,
            status=status,
            timing=[],
            thread_id=thread_id,
            execution_time=0,
            message=int(failed_rows),
            adapter_response={}
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
