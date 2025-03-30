import io
import threading
import time

from dbt.adapters.factory import get_adapter
from dbt.artifacts.schemas.run import RunResult, RunStatus
from dbt.context.providers import generate_runtime_model_context
from dbt.contracts.graph.nodes import SeedNode
from dbt.events.types import ShowNode
from dbt.flags import get_flags
from dbt.task.base import ConfiguredTask
from dbt.task.compile import CompileRunner, CompileTask
from dbt.task.seed import SeedRunner
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note
from dbt_common.exceptions import DbtRuntimeError


class ShowRunner(CompileRunner):
    def __init__(self, config, adapter, node, node_index, num_nodes) -> None:
        super().__init__(config, adapter, node, node_index, num_nodes)
        self.run_ephemeral_models = True

    def execute(self, compiled_node, manifest):
        start_time = time.time()

        # Allow passing in -1 (or any negative number) to get all rows
        limit = None if self.config.args.limit < 0 else self.config.args.limit

        model_context = generate_runtime_model_context(compiled_node, self.config, manifest)
        compiled_node.compiled_code = self.adapter.execute_macro(
            macro_name="get_show_sql",
            macro_resolver=manifest,
            context_override=model_context,
            kwargs={
                "compiled_code": model_context["compiled_code"],
                "sql_header": model_context["config"].get("sql_header"),
                "limit": limit,
            },
        )
        adapter_response, execute_result = self.adapter.execute(
            compiled_node.compiled_code, fetch=True
        )

        end_time = time.time()

        return RunResult(
            node=compiled_node,
            status=RunStatus.Success,
            timing=[],
            thread_id=threading.current_thread().name,
            execution_time=end_time - start_time,
            message=None,
            adapter_response=adapter_response.to_dict(),
            agate_table=execute_result,
            failures=None,
            batch_results=None,
        )


class ShowTask(CompileTask):
    def _runtime_initialize(self):
        if not (self.args.select or getattr(self.args, "inline", None)):
            raise DbtRuntimeError("Either --select or --inline must be passed to show")
        super()._runtime_initialize()

    def get_runner_type(self, node):
        if isinstance(node, SeedNode):
            return SeedRunner
        else:
            return ShowRunner

    def task_end_messages(self, results) -> None:
        is_inline = bool(getattr(self.args, "inline", None))

        if is_inline:
            matched_results = [result for result in results if result.node.name == "inline_query"]
        else:
            matched_results = []
            for result in results:
                if result.node.name in self.selection_arg[0]:
                    matched_results.append(result)
                else:
                    fire_event(
                        Note(msg=f"Excluded node '{result.node.name}' from results"),
                        EventLevel.DEBUG,
                    )

        for result in matched_results:
            table = result.agate_table

            # Hack to get Agate table output as string
            output = io.StringIO()
            if self.args.output == "json":
                table.to_json(path=output)
            else:
                table.print_table(output=output, max_rows=None)

            node_name = result.node.name

            if hasattr(result.node, "version") and result.node.version:
                node_name += f".v{result.node.version}"

            fire_event(
                ShowNode(
                    node_name=node_name,
                    preview=output.getvalue(),
                    is_inline=is_inline,
                    output_format=self.args.output,
                    unique_id=result.node.unique_id,
                    quiet=get_flags().QUIET,
                )
            )

    def _handle_result(self, result) -> None:
        super()._handle_result(result)

        if (
            result.node.is_ephemeral_model
            and type(self) is ShowTask
            and (self.args.select or getattr(self.args, "inline", None))
        ):
            self.node_results.append(result)


class ShowTaskDirect(ConfiguredTask):
    def run(self):
        adapter = get_adapter(self.config)
        with adapter.connection_named("show", should_release_connection=False):
            limit = None if self.args.limit < 0 else self.args.limit
            response, table = adapter.execute(self.args.inline_direct, fetch=True, limit=limit)

            output = io.StringIO()
            if self.args.output == "json":
                table.to_json(path=output)
            else:
                table.print_table(output=output, max_rows=None)

            fire_event(
                ShowNode(
                    node_name="direct-query",
                    preview=output.getvalue(),
                    is_inline=True,
                    output_format=self.args.output,
                    unique_id="direct-query",
                    quiet=get_flags().QUIET,
                )
            )
