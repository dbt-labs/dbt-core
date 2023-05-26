import threading
from typing import AbstractSet, Optional

from dbt.contracts.graph.manifest import WritableManifest
from dbt.contracts.results import RunStatus, RunResult
from dbt.events.base_types import EventLevel
from dbt.events.functions import fire_event
from dbt.events.types import CompiledNode, Note
from dbt.exceptions import DbtInternalError, DbtRuntimeError
from dbt.graph import ResourceTypeSelector
from dbt.node_types import NodeType
from dbt.parser.manifest import write_manifest, process_node
from dbt.parser.sql import SqlBlockParser
from dbt.task.base import BaseRunner
from dbt.task.runnable import GraphRunnableTask


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
    # We add a new inline node to the manifest during initialization
    # it should be removed before the task is complete
    _inline_node_id = None

    def raise_on_first_error(self):
        return True

    def get_node_selector(self) -> ResourceTypeSelector:
        if getattr(self.args, "inline", None):
            resource_types = [NodeType.SqlOperation]
        else:
            resource_types = NodeType.executable()

        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get perform node selection")
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=resource_types,
        )

    def get_runner_type(self, _):
        return CompileRunner

    def task_end_messages(self, results):
        is_inline = bool(getattr(self.args, "inline", None))
        output_format = getattr(self.args, "output", "text")

        if is_inline:
            matched_results = [result for result in results if result.node.name == "inline_query"]
        elif self.selection_arg:
            matched_results = []
            for result in results:
                if result.node.name in self.selection_arg[0]:
                    matched_results.append(result)
                else:
                    fire_event(
                        Note(msg=f"Excluded node '{result.node.name}' from results"),
                        EventLevel.DEBUG,
                    )
        # No selector passed, compiling all nodes
        else:
            matched_results = []

        for result in matched_results:
            fire_event(
                CompiledNode(
                    node_name=result.node.name,
                    compiled=result.node.compiled_code,
                    is_inline=is_inline,
                    output_format=output_format,
                    unique_id=result.node.unique_id,
                )
            )

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
        write_manifest(self.manifest, self.config.target_path)

    def _runtime_initialize(self):
        if getattr(self.args, "inline", None):
            block_parser = SqlBlockParser(
                project=self.config, manifest=self.manifest, root_project=self.config
            )
            sql_node = block_parser.parse_remote(self.args.inline, "inline_query")
            process_node(self.config, self.manifest, sql_node)
            # keep track of the node added to the manifest
            self._inline_node_id = sql_node.unique_id

        super()._runtime_initialize()

    def after_run(self, adapter, results):
        # remove inline node from manifest
        if self._inline_node_id:
            self.manifest.nodes.pop(self._inline_node_id)
            self._inline_node_id = None
        super().after_run(adapter, results)

    def _handle_result(self, result):
        super()._handle_result(result)

        if (
            result.node.is_ephemeral_model
            and type(self) is CompileTask
            and (self.args.select or getattr(self.args, "inline", None))
        ):
            self.node_results.append(result)
