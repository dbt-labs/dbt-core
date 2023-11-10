import threading

from .run import RunTask, ModelRunner as run_model_runner
from .snapshot import SnapshotRunner as snapshot_model_runner
from .seed import SeedRunner as seed_runner
from .test import TestRunner as test_runner

from dbt.adapters.factory import get_adapter
from dbt.contracts.results import NodeStatus
from dbt.exceptions import DbtInternalError
from dbt.graph import ResourceTypeSelector
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
    }
    ALL_RESOURCE_VALUES = frozenset({x for x in RUNNER_MAP.keys()})

    @property
    def resource_types(self):
        if self.args.include_saved_query:
            self.RUNNER_MAP[NodeType.SavedQuery] = SavedQueryRunner
            self.ALL_RESOURCE_VALUES = self.ALL_RESOURCE_VALUES.union({NodeType.SavedQuery})

        if not self.args.resource_types:
            return list(self.ALL_RESOURCE_VALUES)

        values = set(self.args.resource_types)

        if "all" in values:
            values.remove("all")
            values.update(self.ALL_RESOURCE_VALUES)

        return list(values)

    def get_node_selector(self) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get node selection")

        resource_types = self.resource_types

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
