import threading
import time
from dataclasses import replace
from typing import Any, Dict

from dbt.adapters.exceptions import MissingMaterializationError
from dbt.artifacts.schemas.overload_results import OverloadResults
from dbt.artifacts.schemas.results import NodeStatus, RunStatus
from dbt.artifacts.schemas.run import RunResult
from dbt.clients.jinja import MacroGenerator
from dbt.context.providers import generate_runtime_function_context
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import FunctionNode
from dbt.events.types import (
    LogFunctionResult,
    LogOverloadResult,
    LogStartLine,
    LogStartOverload,
)
from dbt.task import group_lookup
from dbt.task.compile import CompileRunner
from dbt_common.clients.jinja import MacroProtocol
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtValidationError


class FunctionRunner(CompileRunner[FunctionNode]):

    def describe_node(self) -> str:
        return f"function {self.get_node_representation()}"

    def before_execute(self) -> None:
        fire_event(
            LogStartLine(
                description=self.describe_node(),
                index=self.node_index,
                total=self.num_nodes,
                node_info=self.node.node_info,
            )
        )

    def _get_materialization_macro(
        self, compiled_node: FunctionNode, manifest: Manifest
    ) -> MacroProtocol:
        materialization_macro = manifest.find_materialization_macro_by_name(
            self.config.project_name, compiled_node.get_materialization(), self.adapter.type()
        )
        if materialization_macro is None:
            raise MissingMaterializationError(
                materialization=compiled_node.get_materialization(),
                adapter_type=self.adapter.type(),
            )

        return materialization_macro

    def _check_lang_supported(
        self, compiled_node: FunctionNode, materialization_macro: MacroProtocol
    ) -> None:
        # TODO: This function and its typing is a bit wonky, we should fix it
        # Specifically, a MacroProtocol doesn't have a supported_languags attribute, but a macro does. We're acting
        # like the materialization_macro might not have a supported_languages attribute, but we access it in an unguarded manner.
        # So are we guaranteed to always have a Macro here? (because a Macro always has a supported_languages attribute)
        # This logic is a copy of of the logic in the run.py file, so the same logical conundrum applies there. Also perhaps
        # we can refactor to having one definition, and maybe a logically consistent one...
        mat_has_supported_langs = hasattr(materialization_macro, "supported_languages")
        function_lang_supported = compiled_node.language in materialization_macro.supported_languages  # type: ignore
        if mat_has_supported_langs and not function_lang_supported:
            str_langs = [str(lang) for lang in materialization_macro.supported_languages]  # type: ignore
            raise DbtValidationError(
                f'Materialization "{materialization_macro.name}" only supports languages {str_langs}; '
                f'got "{compiled_node.language}"'
            )

    def build_result(self, compiled_node: FunctionNode, context: Dict[str, Any]) -> RunResult:
        loaded_result = context["load_result"]("main")

        return RunResult(
            node=compiled_node,
            status=RunStatus.Success,
            timing=[],
            thread_id=threading.current_thread().name,
            # This gets set later in `from_run_result` called by `BaseRunner.safe_run`
            execution_time=0.0,
            message=str(loaded_result.response),
            adapter_response=loaded_result.response.to_dict(omit_none=True),
            failures=loaded_result.get("failures"),
            batch_results=None,
        )

    def execute(self, compiled_node: FunctionNode, manifest: Manifest) -> RunResult:
        materialization_macro = self._get_materialization_macro(compiled_node, manifest)
        self._check_lang_supported(compiled_node, materialization_macro)

        # Skip overloads that already succeeded on a previous run (used by `dbt retry`).
        previous = compiled_node.previous_overload_results
        already_successful = set(previous.successful) if previous else set()

        # Execute the root function
        context = generate_runtime_function_context(compiled_node, self.config, manifest)
        MacroGenerator(materialization_macro, context=context)()

        # Execute each overload with the same database name but different
        # arguments/returns/body. We attempt every overload regardless of
        # earlier failures; the order in which overloads are created does not
        # affect database-side dispatch.
        overload_results = OverloadResults()
        total_overloads = len(compiled_node.overloads)
        group = group_lookup.get(compiled_node.unique_id)
        for idx, overload in enumerate(compiled_node.overloads, start=1):
            overload_desc = f"function {compiled_node.name} overload {overload.defined_in}"

            if overload.defined_in in already_successful:
                overload_results.successful.append(overload.defined_in)
                fire_event(
                    LogOverloadResult(
                        description=overload_desc,
                        status="skipped",
                        overload_index=idx,
                        total_overloads=total_overloads,
                        execution_time=0.0,
                        node_info=self.node.node_info,
                        group=group,
                    ),
                    level=EventLevel.INFO,
                )
                continue

            fire_event(
                LogStartOverload(
                    description=overload_desc,
                    overload_index=idx,
                    total_overloads=total_overloads,
                    node_info=self.node.node_info,
                )
            )

            overload_node = replace(
                compiled_node,
                arguments=overload.arguments,
                returns=overload.returns or compiled_node.returns,
                compiled_code=overload.compiled_body or "",
            )
            overload_ctx = generate_runtime_function_context(overload_node, self.config, manifest)
            overload_started_at = time.time()
            try:
                MacroGenerator(materialization_macro, context=overload_ctx)()
                overload_results.successful.append(overload.defined_in)
                fire_event(
                    LogOverloadResult(
                        description=overload_desc,
                        status=str(RunStatus.Success),
                        overload_index=idx,
                        total_overloads=total_overloads,
                        execution_time=time.time() - overload_started_at,
                        node_info=self.node.node_info,
                        group=group,
                    ),
                    level=EventLevel.INFO,
                )
            except Exception as e:
                overload_results.failed.append(overload.defined_in)
                fire_event(
                    LogOverloadResult(
                        description=f"{overload_desc}: {e}",
                        status=str(RunStatus.Error),
                        overload_index=idx,
                        total_overloads=total_overloads,
                        execution_time=time.time() - overload_started_at,
                        node_info=self.node.node_info,
                        group=group,
                    ),
                    level=EventLevel.ERROR,
                )

        result = self.build_result(compiled_node, context)
        result.overload_results = overload_results

        if overload_results.failed:
            result.status = RunStatus.PartialSuccess
            result.message = f"PARTIAL SUCCESS {compiled_node.name}"

        return result

    def from_run_result(self, result, start_time, timing_info):
        run_result = super().from_run_result(result, start_time, timing_info)
        run_result.overload_results = result.overload_results
        return run_result

    def after_execute(self, result: RunResult) -> None:
        self.print_result_line(result)

    def print_result_line(self, result: RunResult) -> None:
        node = result.node
        assert isinstance(node, FunctionNode)

        group = group_lookup.get(node.unique_id)
        level = EventLevel.ERROR if result.status == NodeStatus.Error else EventLevel.INFO
        fire_event(
            LogFunctionResult(
                description=self.describe_node(),
                status=result.status,
                index=self.node_index,
                total=self.num_nodes,
                execution_time=result.execution_time,
                node_info=self.node.node_info,
                group=group,
            ),
            level=level,
        )
