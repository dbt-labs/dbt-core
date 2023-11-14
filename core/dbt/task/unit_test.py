import agate
from dataclasses import dataclass
from dbt.dataclass_schema import dbtClassMixin
import daff
import threading
import re
from typing import Dict, Any, Optional, AbstractSet, List

from .compile import CompileRunner
from .run import RunTask

from dbt.adapters.factory import get_adapter
from dbt.clients.agate_helper import list_rows_from_table, json_rows_from_table
from dbt.contracts.graph.nodes import UnitTestNode
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import TestStatus, RunResult
from dbt.context.providers import generate_runtime_model_context
from dbt.clients.jinja import MacroGenerator
from dbt.events.functions import fire_event
from dbt.events.types import (
    LogTestResult,
    LogStartLine,
)
from dbt.graph import ResourceTypeSelector
from dbt.exceptions import (
    DbtInternalError,
    MissingMaterializationError,
)
from dbt.node_types import NodeType
from dbt.parser.unit_tests import UnitTestManifestLoader
from dbt.ui import green, red


@dataclass
class UnitTestDiff(dbtClassMixin):
    actual: List[Dict[str, Any]]
    expected: List[Dict[str, Any]]
    rendered: str


@dataclass
class UnitTestResultData(dbtClassMixin):
    should_error: bool
    adapter_response: Dict[str, Any]
    diff: Optional[UnitTestDiff] = None


class UnitTestRunner(CompileRunner):
    _ANSI_ESCAPE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

    def describe_node(self):
        return f"{self.node.resource_type} {self.node.name}"

    def print_result_line(self, result):
        model = result.node

        fire_event(
            LogTestResult(
                name=model.name,
                status=str(result.status),
                index=self.node_index,
                num_models=self.num_nodes,
                execution_time=result.execution_time,
                node_info=model.node_info,
                num_failures=result.failures,
            ),
            level=LogTestResult.status_to_level(str(result.status)),
        )

    def print_start_line(self):
        fire_event(
            LogStartLine(
                description=self.describe_node(),
                index=self.node_index,
                total=self.num_nodes,
                node_info=self.node.node_info,
            )
        )

    def before_execute(self):
        self.print_start_line()

    def execute_unit_test(self, node: UnitTestNode, manifest: Manifest) -> UnitTestResultData:
        # generate_runtime_unit_test_context not strictly needed - this is to run the 'unit'
        # materialization, not compile the node.compiled_code
        context = generate_runtime_model_context(node, self.config, manifest)

        materialization_macro = manifest.find_materialization_macro_by_name(
            self.config.project_name, node.get_materialization(), self.adapter.type()
        )

        if materialization_macro is None:
            raise MissingMaterializationError(
                materialization=node.get_materialization(), adapter_type=self.adapter.type()
            )

        if "config" not in context:
            raise DbtInternalError(
                "Invalid materialization context generated, missing config: {}".format(context)
            )

        # generate materialization macro
        macro_func = MacroGenerator(materialization_macro, context)
        # execute materialization macro
        macro_func()
        # load results from context
        # could eventually be returned directly by materialization
        result = context["load_result"]("main")
        adapter_response = result["response"].to_dict(omit_none=True)
        table = result["table"]
        actual = self._get_unit_test_agate_table(table, "actual")
        expected = self._get_unit_test_agate_table(table, "expected")

        # generate diff, if exists
        should_error, diff = False, None
        daff_diff = self._get_daff_diff(expected, actual)
        if daff_diff.hasDifference():
            should_error = True
            rendered = self._render_daff_diff(daff_diff)
            rendered = f"\n\n{red('expected')} differs from {green('actual')}:\n\n{rendered}\n"

            diff = UnitTestDiff(
                actual=json_rows_from_table(actual),
                expected=json_rows_from_table(expected),
                rendered=rendered,
            )

        return UnitTestResultData(
            diff=diff,
            should_error=should_error,
            adapter_response=adapter_response,
        )

    def execute(self, node: UnitTestNode, manifest: Manifest):
        result = self.execute_unit_test(node, manifest)
        thread_id = threading.current_thread().name

        status = TestStatus.Pass
        message = None
        failures = 0
        if result.should_error:
            status = TestStatus.Fail
            message = result.diff.rendered if result.diff else None
            failures = 1

        return RunResult(
            node=node,
            status=status,
            timing=[],
            thread_id=thread_id,
            execution_time=0,
            message=message,
            adapter_response=result.adapter_response,
            failures=failures,
        )

    def after_execute(self, result):
        self.print_result_line(result)

    def _get_unit_test_agate_table(self, result_table, actual_or_expected: str) -> agate.Table:
        unit_test_table = result_table.where(
            lambda row: row["actual_or_expected"] == actual_or_expected
        )
        columns = list(unit_test_table.columns.keys())
        columns.remove("actual_or_expected")
        return unit_test_table.select(columns)

    def _get_daff_diff(
        self, expected: agate.Table, actual: agate.Table, ordered: bool = False
    ) -> daff.TableDiff:

        expected_daff_table = daff.PythonTableView(list_rows_from_table(expected))
        actual_daff_table = daff.PythonTableView(list_rows_from_table(actual))

        alignment = daff.Coopy.compareTables(expected_daff_table, actual_daff_table).align()
        result = daff.PythonTableView([])

        flags = daff.CompareFlags()
        flags.ordered = ordered

        diff = daff.TableDiff(alignment, flags)
        diff.hilite(result)
        return diff

    def _render_daff_diff(self, daff_diff: daff.TableDiff) -> str:
        result = daff.PythonTableView([])
        daff_diff.hilite(result)
        rendered = daff.TerminalDiffRender().render(result)
        # strip colors if necessary
        if not self.config.args.use_colors:
            rendered = self._ANSI_ESCAPE.sub("", rendered)

        return rendered


class UnitTestSelector(ResourceTypeSelector):
    # This is what filters out nodes except Unit Tests, in filter_selection
    def __init__(self, graph, manifest, previous_state):
        super().__init__(
            graph=graph,
            manifest=manifest,
            previous_state=previous_state,
            resource_types=[NodeType.Unit],
        )


class UnitTestTask(RunTask):
    """
    Unit testing:
        Read schema files + custom data tests and validate that
        constraints are satisfied.
    """

    def __init__(self, args, config, manifest):
        # This will initialize the RunTask with the regular manifest
        super().__init__(args, config, manifest)
        # TODO: We might not need this, but leaving here for now.
        self.original_manifest = manifest
        self.using_unit_test_manifest = False

    __test__ = False

    def raise_on_first_error(self):
        return False

    @property
    def selection_arg(self):
        if self.using_unit_test_manifest is False:
            return self.args.select
        else:
            # Everything in the unit test should be selected, since we
            # created in from a selection list.
            return ()

    @property
    def exclusion_arg(self):
        if self.using_unit_test_manifest is False:
            return self.args.exclude
        else:
            # Everything in the unit test should be selected, since we
            # created in from a selection list.
            return ()

    def build_unit_test_manifest(self):
        loader = UnitTestManifestLoader(self.manifest, self.config, self.job_queue._selected)
        return loader.load()

    def reset_job_queue_and_manifest(self):
        # We want deferral to happen here (earlier than normal) before we turn
        # the normal manifest into the unit testing manifest
        adapter = get_adapter(self.config)
        with adapter.connection_named("master"):
            self.populate_adapter_cache(adapter)
            self.defer_to_manifest(adapter, self.job_queue._selected)

        # We have the selected models from the "regular" manifest, now we switch
        # to using the unit_test_manifest to run the unit tests.
        self.using_unit_test_manifest = True
        self.manifest = self.build_unit_test_manifest()
        self.compile_manifest()  # create the networkx graph
        self.job_queue = self.get_graph_queue()

    def before_run(self, adapter, selected_uids: AbstractSet[str]) -> None:
        # We already did cache population + deferral earlier (in reset_job_queue_and_manifest)
        # and we don't need to create any schemas
        pass

    def get_node_selector(self) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get perform node selection")
        # Filter out everything except unit tests
        return UnitTestSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
        )

    def get_runner_type(self, _):
        return UnitTestRunner
