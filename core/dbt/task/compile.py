import threading
from typing import AbstractSet, Optional

from .runnable import GraphRunnableTask
from .base import BaseRunner

from dbt.contracts.graph.manifest import WritableManifest
from dbt.contracts.results import RunStatus, RunResult
from dbt.exceptions import DbtInternalError, DbtRuntimeError
from dbt.graph import ResourceTypeSelector
from dbt.events.functions import fire_event
from dbt.events.types import CompileComplete
from dbt.node_types import NodeType


class CompileRunner(BaseRunner):
    def before_execute(self):
        pass

    def after_execute(self, result):
        pass

    def execute(self, compiled_node, manifest):
        return RunResult(
            node=compiled_node,
            status=RunStatus.Success,
            timing=[],
            thread_id=threading.current_thread().name,
            execution_time=0,
            message=None,
            adapter_response={},
            failures=None,
        )

    def compile(self, manifest):
        compiler = self.adapter.get_compiler()
        return compiler.compile_node(self.node, manifest, {})


class CompileTask(GraphRunnableTask):
    def raise_on_first_error(self):
        return True

    def get_node_selector(self) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get perform node selection")
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=NodeType.executable(),
        )

    def get_runner_type(self, _):
        return CompileRunner

    def task_end_messages(self, results):
        fire_event(CompileComplete())

    def _get_deferred_manifest(self) -> Optional[WritableManifest]:
        if not self.args.defer:
            return None

        state = self.previous_state
        if state is None:
            raise DbtRuntimeError(
                "Received a --defer argument, but no value was provided to --state"
            )

        if state.manifest is None:
            raise DbtRuntimeError(f'Could not find manifest in --state path: "{self.args.state}"')
        return state.manifest

    def defer_to_manifest(self, adapter, selected_uids: AbstractSet[str]):
        deferred_manifest = self._get_deferred_manifest()
        if deferred_manifest is None:
            return
        if self.manifest is None:
            raise DbtInternalError(
                "Expected to defer to manifest, but there is no runtime manifest to defer from!"
            )
        self.manifest.merge_from_artifact(
            adapter=adapter,
            other=deferred_manifest,
            selected=selected_uids,
            favor_state=bool(self.args.favor_state),
        )
        # TODO: is it wrong to write the manifest here? I think it's right...
        self.write_manifest()
