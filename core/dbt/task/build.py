import threading

from .run import RunTask, ModelRunner as run_model_runner
from .snapshot import SnapshotRunner as snapshot_model_runner
from .seed import SeedRunner as seed_runner
from .test import TestRunner as test_runner

from dbt.adapters.factory import get_adapter
from dbt.contracts.results import NodeStatus
from dbt.exceptions import DbtInternalError
from dbt.graph import ResourceTypeSelector, GraphQueue
from dbt.node_types import NodeType
from dbt.task.test import TestSelector
from dbt.task.base import BaseRunner
from dbt.contracts.results import RunResult, RunStatus
from dbt.events.functions import fire_event
from dbt.events.types import LogStartLine, LogModelResult
from dbt.events.base_types import EventLevel


class SavedQueryRunner(BaseRunner):
    # A no-op Runner for Saved Queries
    @property
    def description(self):
        return "Saved Query {}".format(self.node.unique_id)

    def before_execute(self):
        fire_event(
            LogStartLine(
                description=self.description,
                index=self.node_index,
                total=self.num_nodes,
                node_info=self.node.node_info,
            )
        )

    def compile(self, manifest):
        return self.node

    def after_execute(self, result):
        if result.status == NodeStatus.Error:
            level = EventLevel.ERROR
        else:
            level = EventLevel.INFO
        fire_event(
            LogModelResult(
                description=self.description,
                status=result.status,
                index=self.node_index,
                total=self.num_nodes,
                execution_time=result.execution_time,
                node_info=self.node.node_info,
            ),
            level=level,
        )

    def execute(self, compiled_node, manifest):
        # no-op
        return RunResult(
            node=compiled_node,
            status=RunStatus.Success,
            timing=[],
            thread_id=threading.current_thread().name,
            execution_time=0.1,
            message="done",
            adapter_response={},
            failures=0,
            agate_table=None,
        )


class BuildTask(RunTask):
    """The Build task processes all assets of a given process and attempts to
    'build' them in an opinionated fashion.  Every resource type outlined in
    RUNNER_MAP will be processed by the mapped runner class.

    I.E. a resource of type Model is handled by the ModelRunner which is
    imported as run_model_runner."""

    MARK_DEPENDENT_ERRORS_STATUSES = [NodeStatus.Error, NodeStatus.Fail]

    RUNNER_MAP = {
        NodeType.Model: run_model_runner,
        NodeType.Snapshot: snapshot_model_runner,
        NodeType.Seed: seed_runner,
        NodeType.Test: test_runner,
        NodeType.Unit: test_runner,
    }
    ALL_RESOURCE_VALUES = frozenset({x for x in RUNNER_MAP.keys()})

    def resource_types(self, no_unit_tests=False):
        if self.args.include_saved_query:
            self.RUNNER_MAP[NodeType.SavedQuery] = SavedQueryRunner
            self.ALL_RESOURCE_VALUES = self.ALL_RESOURCE_VALUES.union({NodeType.SavedQuery})

        if not self.args.resource_types:
            resource_types = list(self.ALL_RESOURCE_VALUES)
        else:
            resource_types = set(self.args.resource_types)

            if "all" in resource_types:
                resource_types.remove("all")
                resource_types.update(self.ALL_RESOURCE_VALUES)

        if no_unit_tests is True and NodeType.Unit in resource_types:
            resource_types.remove(NodeType.Unit)
        return list(resource_types)

    def get_graph_queue(self) -> GraphQueue:
        # Following uses self.selection_arg and self.exclusion_arg
        spec = self.get_selection_spec()

        # selector including unit tests
        full_selector = self.get_node_selector(no_unit_tests=False)
        # selected node unique_ids with unit_tests
        full_selected_nodes = full_selector.get_selected(spec)

        # This selector removes the unit_tests from the selector
        selector_wo_unit_tests = self.get_node_selector(no_unit_tests=True)
        # selected node unique_ids without unit_tests
        selected_nodes_wo_unit_tests = selector_wo_unit_tests.get_selected(spec)

        # Get the difference in the sets of nodes with and without unit tests and
        # save it
        selected_unit_tests = full_selected_nodes - selected_nodes_wo_unit_tests
        self.build_model_to_unit_test_map(selected_unit_tests)

        # get_graph_queue in the selector will remove NodeTypes not specified
        # in the node_selector (filter_selection).
        return selector_wo_unit_tests.get_graph_queue(spec)

    def build_model_to_unit_test_map(self, selected_unit_tests):
        dct = {}
        for unit_test_unique_id in selected_unit_tests:
            unit_test = self.manifest.unit_tests[unit_test_unique_id]
            model_unique_id = unit_test.depends_on.nodes[0]
            if model_unique_id not in dct:
                dct[model_unique_id] = []
            dct[model_unique_id].append(unit_test.unique_id)
        self.model_to_unit_test_map = dct

    def get_node_selector(self, no_unit_tests=False) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get node selection")

        resource_types = self.resource_types(no_unit_tests)

        if resource_types == [NodeType.Test]:
            return TestSelector(
                graph=self.graph,
                manifest=self.manifest,
                previous_state=self.previous_state,
            )
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=resource_types,
        )

    def get_runner_type(self, node):
        return self.RUNNER_MAP.get(node.resource_type)

    def compile_manifest(self):
        if self.manifest is None:
            raise DbtInternalError("compile_manifest called before manifest was loaded")
        adapter = get_adapter(self.config)
        compiler = adapter.get_compiler()
        self.graph = compiler.compile(self.manifest, add_test_edges=True)
