import io
import threading

from dbt.contracts.results import RunResult, RunStatus
from dbt.events.functions import fire_event
from dbt.events.types import ShowNodeText, ShowNodeJson
from dbt.exceptions import DbtRuntimeError
from dbt.task.compile import CompileTask, CompileRunner


class ShowRunner(CompileRunner):
    def execute(self, compiled_node, manifest):
        adapter_response, execute_result = self.adapter.execute(
            compiled_node.compiled_code, fetch=True
        )

        return RunResult(
            node=compiled_node,
            status=RunStatus.Success,
            timing=[],
            thread_id=threading.current_thread().name,
            execution_time=0,
            message=None,
            adapter_response=adapter_response.to_dict(),
            agate_table=execute_result,
            failures=None,
        )


class ShowTask(CompileTask):
    def _runtime_initialize(self):
        if not self.args.select or getattr(self.args, "inline", None):
            raise DbtRuntimeError("Either --select or --inline must be passed to show")
        super()._runtime_initialize()

    def get_runner_type(self, _):
        return ShowRunner

    def task_end_messages(self, results):
        if getattr(self.args, "inline", None):
            matched_results = results
            is_inline = True
        else:
            matched_results = [
                result for result in results if result.node.name == self.selection_arg[0]
            ]
            is_inline = False

        result = matched_results[0]

        # Allow passing in -1 (or any negative number) to get all rows
        table = result.agate_table

        if self.args.limit >= 0:
            table = table.limit(self.args.limit)

        # Hack to get Agate table output as string
        output = io.StringIO()
        if self.args.output == "json":
            table.to_json(path=output)
            fire_event(
                ShowNodeJson(
                    node_name=result.node.name, preview=output.getvalue(), is_inline=is_inline
                )
            )
        else:
            table.print_table(output=output, max_rows=None)
            fire_event(
                ShowNodeText(
                    node_name=result.node.name, preview=output.getvalue(), is_inline=is_inline
                )
            )
