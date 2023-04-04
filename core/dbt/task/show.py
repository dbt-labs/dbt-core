import io
import threading
import time

from dbt.contracts.results import RunResult, RunStatus
from dbt.events.base_types import EventLevel
from dbt.events.functions import fire_event
from dbt.events.types import ShowNode, Note
from dbt.exceptions import DbtRuntimeError
from dbt.task.compile import CompileTask, CompileRunner


class ShowRunner(CompileRunner):
    def __init__(self, config, adapter, node, node_index, num_nodes):
        super().__init__(config, adapter, node, node_index, num_nodes)
        self.run_ephemeral_models = True

    def execute(self, compiled_node, manifest):
        start_time = time.time()
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
        )


class ShowTask(CompileTask):
    def _runtime_initialize(self):
        if not (self.args.select or getattr(self.args, "inline", None)):
            raise DbtRuntimeError("Either --select or --inline must be passed to show")
        super()._runtime_initialize()

    def get_runner_type(self, _):
        return ShowRunner

    def task_end_messages(self, results):
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
            # Allow passing in -1 (or any negative number) to get all rows
            table = result.agate_table

            if self.args.limit >= 0:
                table = table.limit(self.args.limit)
                result.agate_table = table

            # Hack to get Agate table output as string
            output = io.StringIO()
            if self.args.output == "json":
                table.to_json(path=output)
            else:
                table.print_table(output=output, max_rows=None)

            fire_event(
                ShowNode(
                    node_name=result.node.name,
                    preview=output.getvalue(),
                    is_inline=is_inline,
                    output_format=self.args.output,
                )
            )

    def _handle_result(self, result):
        super()._handle_result(result)

        if (
            result.node.is_ephemeral_model
            and type(self) is ShowTask
            and (self.args.select or getattr(self.args, "inline", None))
        ):
            self.node_results.append(result)
