import random

from .run import ModelRunner, RunTask
from .printer import (
    print_run_end_messages,
)

from dbt.contracts.results import RunStatus
from dbt.exceptions import InternalException
from dbt.graph import ResourceTypeSelector
from dbt.logger import TextOnly
from dbt.events.functions import fire_event
from dbt.events.types import (
    SeedHeader,
    SeedHeaderSeparator,
    EmptyLine,
    PrintSeedErrorResultLine,
    PrintSeedResultLine,
    PrintStartLine,
)
from dbt.node_types import NodeType
from dbt.contracts.results import NodeStatus


class SeedRunner(ModelRunner):
    def describe_node(self):
        return "seed file {}".format(self.get_node_representation())

    def before_execute(self):
        fire_event(
            PrintStartLine(
                description=self.describe_node(),
                index=self.node_index,
                total=self.num_nodes,
                node_info=self.node.node_info,
            )
        )

    def _build_run_model_result(self, model, context):
        result = super()._build_run_model_result(model, context)
        agate_result = context["load_result"]("agate_table")
        result.agate_table = agate_result.table
        return result

    def compile(self, manifest):
        return self.node

    def print_result_line(self, result):
        model = result.node
        if result.status == NodeStatus.Error:
            fire_event(
                PrintSeedErrorResultLine(
                    status=result.status,
                    index=self.node_index,
                    total=self.num_nodes,
                    execution_time=result.execution_time,
                    schema=self.node.schema,
                    relation=model.alias,
                    node_info=model.node_info,
                )
            )
        else:
            fire_event(
                PrintSeedResultLine(
                    status=result.message,
                    index=self.node_index,
                    total=self.num_nodes,
                    execution_time=result.execution_time,
                    schema=self.node.schema,
                    relation=model.alias,
                    node_info=model.node_info,
                )
            )


class SeedTask(RunTask):
    def defer_to_manifest(self, adapter, selected_uids):
        # seeds don't defer
        return

    def raise_on_first_error(self):
        return False

    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise InternalException("manifest and graph must be set to get perform node selection")
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[NodeType.Seed],
        )

    def get_runner_type(self, _):
        return SeedRunner

    def task_end_messages(self, results):
        if self.args.show:
            self.show_tables(results)

        print_run_end_messages(results)

    def show_table(self, result):
        table = result.agate_table
        rand_table = table.order_by(lambda x: random.random())

        schema = result.node.schema
        alias = result.node.alias

        header = "Random sample of table: {}.{}".format(schema, alias)
        with TextOnly():
            fire_event(EmptyLine())
        fire_event(SeedHeader(header=header))
        fire_event(SeedHeaderSeparator(len_header=len(header)))

        rand_table.print_table(max_rows=10, max_columns=None)
        with TextOnly():
            fire_event(EmptyLine())

    def show_tables(self, results):
        for result in results:
            if result.status != RunStatus.Error:
                self.show_table(result)
