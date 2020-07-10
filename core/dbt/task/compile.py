from .runnable import GraphRunnableTask
from .base import BaseRunner

from dbt.compilation import compile_node
from dbt.contracts.results import RunModelResult
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
        return RunModelResult(compiled_node)

    def compile(self, manifest):
        return compile_node(self.adapter, self.config, self.node, manifest, {})


class CompileTask(GraphRunnableTask):
    def raise_on_first_error(self):
        return True

    def get_selection_spec(self) -> SelectionSpec:
        if self.args.selector_name:
            spec = self.config.get_selector(self.args.selector_name)
        else:
            spec = parse_difference(self.args.models, self.args.exclude)
        return spec

    def get_node_selector(self) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            raise InternalException(
                'manifest and graph must be set to get perform node selection'
            )
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            resource_types=NodeType.executable(),
        )

    def get_runner_type(self):
        return CompileRunner

    def task_end_messages(self, results):
        print_timestamped_line('Done.')
