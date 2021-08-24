import threading
from .runnable import GraphRunnableTask
from .base import BaseRunner

from dbt.contracts.results import RunStatus, RunResult
from dbt.exceptions import InternalException
from dbt.graph import ResourceTypeSelector, SelectionSpec, parse_difference
from dbt.logger import print_timestamped_line
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
            failures=None
        )

    def compile(self, manifest):
        compiler = self.adapter.get_compiler()
        return compiler.compile_node(self.node, manifest, {})


class CompileTask(GraphRunnableTask):
    def raise_on_first_error(self):
        return True

    def get_selection_spec(self) -> SelectionSpec:
        if self.args.selector_name:
            spec = self.config.get_selector(self.args.selector_name)
        else:
            spec = parse_difference(self.args.select, self.args.exclude)
        return spec

    def get_node_selector(self) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            raise InternalException(
                'manifest and graph must be set to get perform node selection'
            )
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=NodeType.executable(),
        )

    def get_runner_type(self, _):
        return CompileRunner

    def task_end_messages(self, results):
        print_timestamped_line('Done.')
