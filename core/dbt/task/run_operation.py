import os
import threading
import traceback
from datetime import datetime, timezone
from typing import TYPE_CHECKING, List

import dbt_common.exceptions
from dbt.adapters.factory import get_adapter
from dbt.artifacts.schemas.results import RunStatus, TimingInfo, collect_timing_info
from dbt.artifacts.schemas.run import RunResult, RunResultsArtifact
from dbt.constants import RUN_RESULTS_FILE_NAME
from dbt.contracts.files import FileHash
from dbt.contracts.graph.nodes import HookNode
from dbt.events.types import (
    ArtifactWritten,
    LogDebugStackTrace,
    RunningOperationCaughtError,
    RunningOperationUncaughtError,
)
from dbt.node_types import NodeType
from dbt.task.base import ConfiguredTask
from dbt_common.events.functions import fire_event

if TYPE_CHECKING:
    import agate


class RunOperationTask(ConfiguredTask):
    def _get_macro_parts(self):
        macro_name = self.args.macro
        if "." in macro_name:
            package_name, macro_name = macro_name.split(".", 1)
        else:
            package_name = None

        return package_name, macro_name

    def _run_unsafe(self, package_name, macro_name) -> "agate.Table":
        adapter = get_adapter(self.config)

        macro_kwargs = self.args.args

        with adapter.connection_named("macro_{}".format(macro_name)):
            adapter.clear_transaction()
            res = adapter.execute_macro(
                macro_name, project=package_name, kwargs=macro_kwargs, macro_resolver=self.manifest
            )

        return res

    def _run_inline_unsafe(self):
        from dbt.parser.manifest import process_node
        from dbt.parser.sql import SqlBlockParser

        adapter = get_adapter(self.config)

        block_parser = SqlBlockParser(
            project=self.config, manifest=self.manifest, root_project=self.config
        )
        sql_node = block_parser.parse_remote(self.args.inline, "inline_query")
        process_node(self.config, self.manifest, sql_node)

        # Compile the node to resolve Jinja (ref, source, var, etc.)
        compiled_node = self.compiler.compile_node(
            sql_node, self.manifest, extra_context=None, write=False
        )

        with adapter.connection_named("inline_query"):
            adapter.clear_transaction()
            adapter.execute(compiled_node.compiled_code, auto_begin=True, fetch=False)

    def _is_inline(self):
        return bool(getattr(self.args, "inline", None))

    def run(self) -> RunResultsArtifact:
        inline = self._is_inline()
        macro_name = getattr(self.args, "macro", None)

        if not inline and not macro_name:
            raise dbt_common.exceptions.DbtRuntimeError(
                "Either a macro name or --inline must be passed to run-operation"
            )
        if inline and macro_name:
            raise dbt_common.exceptions.DbtRuntimeError(
                "Cannot specify both a macro name and --inline for run-operation"
            )

        timing: List[TimingInfo] = []

        with collect_timing_info("compile", timing.append):
            self.compile_manifest()

        start = timing[0].started_at

        success = True
        error_message = None

        if inline:
            operation_name = "inline_query"
            unique_id = f"sqloperation.{self.config.project_name}.inline_query"
            fqn = unique_id.split(".")
        else:
            package_name, operation_name = self._get_macro_parts()

        with collect_timing_info("execute", timing.append):
            try:
                if inline:
                    self._run_inline_unsafe()
                else:
                    self._run_unsafe(package_name, operation_name)
            except dbt_common.exceptions.DbtBaseException as exc:
                fire_event(RunningOperationCaughtError(exc=str(exc)))
                fire_event(LogDebugStackTrace(exc_info=traceback.format_exc()))
                success = False
                error_message = str(exc)
            except Exception as exc:
                fire_event(RunningOperationUncaughtError(exc=str(exc)))
                fire_event(LogDebugStackTrace(exc_info=traceback.format_exc()))
                success = False
                error_message = str(exc)

        end = timing[1].completed_at

        if not inline:
            macro = (
                self.manifest.find_macro_by_name(
                    operation_name, self.config.project_name, package_name
                )
                if self.manifest
                else None
            )

            if macro:
                unique_id = macro.unique_id
                fqn = unique_id.split(".")
            else:
                raise dbt_common.exceptions.UndefinedMacroError(
                    f"dbt could not find a macro with the name '{operation_name}' in any package"
                )

        execution_time = (end - start).total_seconds() if start and end else 0.0

        run_result = RunResult(
            adapter_response={},
            status=RunStatus.Success if success else RunStatus.Error,
            execution_time=execution_time,
            failures=0 if success else 1,
            message=error_message,
            node=HookNode(
                alias=operation_name,
                checksum=FileHash.from_contents(unique_id),
                database=self.config.credentials.database,
                schema=self.config.credentials.schema,
                resource_type=NodeType.Operation,
                fqn=fqn,
                name=operation_name,
                unique_id=unique_id,
                package_name=self.config.project_name if inline else package_name,
                path="",
                original_file_path="",
            ),
            thread_id=threading.current_thread().name,
            timing=timing,
            batch_results=None,
        )

        results = RunResultsArtifact.from_execution_results(
            generated_at=end or datetime.now(timezone.utc).replace(tzinfo=None),
            elapsed_time=execution_time,
            args={
                k: v
                for k, v in self.args.__dict__.items()
                if k.islower() and type(v) in (str, int, float, bool, list, dict)
            },
            results=[run_result],
        )

        result_path = os.path.join(self.config.project_target_path, RUN_RESULTS_FILE_NAME)

        if self.args.write_json:
            results.write(result_path)
            fire_event(
                ArtifactWritten(
                    artifact_type=results.__class__.__name__, artifact_path=result_path
                )
            )

        return results

    @classmethod
    def interpret_results(cls, results):
        return results.results[0].status == RunStatus.Success
