from typing import Dict, List, Optional, Set, Type

from dbt.artifacts.schemas.results import NodeStatus
from dbt.artifacts.schemas.run import RunResult
from dbt.cli.flags import Flags
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.exceptions import DbtInternalError
from dbt.graph import Graph, GraphQueue, ResourceTypeSelector
from dbt.node_types import NodeType
from dbt.runners import ExposureRunner as exposure_runner
from dbt.runners import SavedQueryRunner as saved_query_runner
from dbt.task.base import BaseRunner, resource_types_from_args
from dbt.task.run import MicrobatchModelRunner

from .run import ModelRunner as run_model_runner
from .run import RunTask
from .seed import SeedRunner as seed_runner
from .snapshot import SnapshotRunner as snapshot_model_runner
from .test import TestRunner as test_runner


class BuildTask(RunTask):
    """The Build task processes all assets of a given process and attempts to
    'build' them in an opinionated fashion.  Every resource type outlined in
    RUNNER_MAP will be processed by the mapped runners class.

    I.E. a resource of type Model is handled by the ModelRunner which is
    imported as run_model_runner."""

    MARK_DEPENDENT_ERRORS_STATUSES = [
        NodeStatus.Error,
        NodeStatus.Fail,
        NodeStatus.Skipped,
        NodeStatus.PartialSuccess,
    ]

    RUNNER_MAP = {
        NodeType.Model: run_model_runner,
        NodeType.Snapshot: snapshot_model_runner,
        NodeType.Seed: seed_runner,
        NodeType.Test: test_runner,
        NodeType.Unit: test_runner,
        NodeType.SavedQuery: saved_query_runner,
        NodeType.Exposure: exposure_runner,
    }
    ALL_RESOURCE_VALUES = frozenset({x for x in RUNNER_MAP.keys()})

    def __init__(self, args: Flags, config: RuntimeConfig, manifest: Manifest) -> None:
        super().__init__(args, config, manifest)
        self.selected_unit_tests: Set = set()
        self.model_to_unit_test_map: Dict[str, List] = {}

    def resource_types(self, no_unit_tests: bool = False) -> List[NodeType]:
        resource_types = resource_types_from_args(
            self.args, set(self.ALL_RESOURCE_VALUES), set(self.ALL_RESOURCE_VALUES)
        )

        # First we get selected_nodes including unit tests, then without,
        # and do a set difference.
        if no_unit_tests is True and NodeType.Unit in resource_types:
            resource_types.remove(NodeType.Unit)
        return list(resource_types)

    # overrides get_graph_queue in runnable.py
    def get_graph_queue(self) -> GraphQueue:
        # Following uses self.selection_arg and self.exclusion_arg
        spec = self.get_selection_spec()

        # selector including unit tests
        full_selector = self.get_node_selector(no_unit_tests=False)
        # selected node unique_ids with unit_tests
        full_selected_nodes = full_selector.get_selected(spec=spec, warn_on_no_nodes=False)

        # This selector removes the unit_tests from the selector
        selector_wo_unit_tests = self.get_node_selector(no_unit_tests=True)
        # selected node unique_ids without unit_tests
        selected_nodes_wo_unit_tests = selector_wo_unit_tests.get_selected(
            spec=spec, warn_on_no_nodes=False
        )

        # Get the difference in the sets of nodes with and without unit tests and
        # save it
        selected_unit_tests = full_selected_nodes - selected_nodes_wo_unit_tests
        self.selected_unit_tests = selected_unit_tests
        self.build_model_to_unit_test_map(selected_unit_tests)

        # get_graph_queue in the selector will remove NodeTypes not specified
        # in the node_selector (filter_selection).
        return selector_wo_unit_tests.get_graph_queue(spec)

    # overrides handle_job_queue in runnable.py
    def handle_job_queue(self, pool, callback):
        if self.run_count == 0:
            self.num_nodes = self.num_nodes + len(self.selected_unit_tests)
        node = self.job_queue.get()
        if (
            node.resource_type == NodeType.Model
            and self.model_to_unit_test_map
            and node.unique_id in self.model_to_unit_test_map
        ):
            self.handle_model_with_unit_tests_node(node, pool, callback)

        else:
            self.handle_job_queue_node(node, pool, callback)

    def handle_model_with_unit_tests_node(self, node, pool, callback):
        self._raise_set_error()
        args = [node, pool]
        if self.config.args.single_threaded:
            callback(self.call_model_and_unit_tests_runner(*args))
        else:
            pool.apply_async(self.call_model_and_unit_tests_runner, args=args, callback=callback)

    def call_model_and_unit_tests_runner(self, node, pool) -> RunResult:
        assert self.manifest
        for unit_test_unique_id in self.model_to_unit_test_map[node.unique_id]:
            unit_test_node = self.manifest.unit_tests[unit_test_unique_id]
            unit_test_runner = self.get_runner(unit_test_node)
            # If the model is marked skip, also skip the unit tests
            if node.unique_id in self._skipped_children:
                # cause is only for ephemeral nodes
                unit_test_runner.do_skip(cause=None)
            result = self.call_runner(unit_test_runner)
            self._handle_result(result)
            if result.status in self.MARK_DEPENDENT_ERRORS_STATUSES:
                # The _skipped_children dictionary can contain a run_result for ephemeral nodes,
                # but that should never be the case here.
                self._skipped_children[node.unique_id] = None
        runner = self.get_runner(node)
        if runner.node.unique_id in self._skipped_children:
            cause = self._skipped_children.pop(runner.node.unique_id)
            runner.do_skip(cause=cause)

        if isinstance(runner, MicrobatchModelRunner):
            runner.set_parent_task(self)
            runner.set_pool(pool)

        return self.call_runner(runner)

    # handle non-model-plus-unit-tests nodes
    def handle_job_queue_node(self, node, pool, callback):
        self._raise_set_error()
        runner = self.get_runner(node)
        # we finally know what we're running! Make sure we haven't decided
        # to skip it due to upstream failures
        if runner.node.unique_id in self._skipped_children:
            cause = self._skipped_children.pop(runner.node.unique_id)
            runner.do_skip(cause=cause)

        if isinstance(runner, MicrobatchModelRunner):
            runner.set_parent_task(self)
            runner.set_pool(pool)

        args = [runner]
        self._submit(pool, args, callback)

    # Make a map of model unique_ids to selected unit test unique_ids,
    # for processing before the model.
    def build_model_to_unit_test_map(self, selected_unit_tests):
        dct = {}
        for unit_test_unique_id in selected_unit_tests:
            unit_test = self.manifest.unit_tests[unit_test_unique_id]
            model_unique_id = unit_test.depends_on.nodes[0]
            if model_unique_id not in dct:
                dct[model_unique_id] = []
            dct[model_unique_id].append(unit_test.unique_id)
        self.model_to_unit_test_map = dct

    # We return two different kinds of selectors, one with unit tests and one without
    def get_node_selector(self, no_unit_tests=False) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get node selection")

        resource_types = self.resource_types(no_unit_tests)

        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=resource_types,
        )

    def get_runner_type(self, node) -> Optional[Type[BaseRunner]]:
        if (
            node.resource_type == NodeType.Model
            and super().get_runner_type(node) == MicrobatchModelRunner
        ):
            return MicrobatchModelRunner

        return self.RUNNER_MAP.get(node.resource_type)

    # Special build compile_manifest method to pass add_test_edges to the compiler
    def compile_manifest(self) -> None:
        if self.manifest is None:
            raise DbtInternalError("compile_manifest called before manifest was loaded")
        self.graph: Graph = self.compiler.compile(self.manifest, add_test_edges=True)
