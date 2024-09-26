import random
from typing import Optional, Type

from dbt.artifacts.schemas.results import NodeStatus, RunStatus
from dbt.contracts.graph.manifest import Manifest
from dbt.events.types import LogSeedResult, LogStartLine, SeedHeader
from dbt.graph import ResourceTypeSelector
from dbt.node_types import NodeType
from dbt.task.base import BaseRunner
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Formatting
from dbt_common.exceptions import DbtInternalError

from .printer import print_run_end_messages
from .run import ModelRunner, RunTask


class SeedRunner(ModelRunner):
    def describe_node(self) -> str:
        return "seed file {}".format(self.get_node_representation())

    def before_execute(self) -> None:
        fire_event(
            LogStartLine(
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

    def compile(self, manifest: Manifest):
        return self.node

    def print_result_line(self, result):
        model = result.node
        level = EventLevel.ERROR if result.status == NodeStatus.Error else EventLevel.INFO
        fire_event(
            LogSeedResult(
                status=result.status,
                result_message=result.message,
                index=self.node_index,
                total=self.num_nodes,
                execution_time=result.execution_time,
                schema=self.node.schema,
                relation=model.alias,
                node_info=model.node_info,
            ),
            level=level,
        )


class SeedTask(RunTask):
    def raise_on_first_error(self) -> bool:
        return False

    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get perform node selection")
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[NodeType.Seed],
        )

    def get_runner_type(self, _) -> Optional[Type[BaseRunner]]:
        return SeedRunner

    def task_end_messages(self, results) -> None:
        if self.args.show:
            self.show_tables(results)

        print_run_end_messages(results)

    def show_table(self, result):
        table = result.agate_table
        rand_table = table.order_by(lambda x: random.random())

        schema = result.node.schema
        alias = result.node.alias

        header = "Random sample of table: {}.{}".format(schema, alias)
        fire_event(Formatting(""))
        fire_event(SeedHeader(header=header))
        fire_event(Formatting("-" * len(header)))

        rand_table.print_table(max_rows=10, max_columns=None)
        fire_event(Formatting(""))

    def show_tables(self, results):
        for result in results:
            if result.status != RunStatus.Error:
                self.show_table(result)
