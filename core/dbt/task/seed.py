import random

from .run import ModelRunner, RunTask
from .printer import (
    print_start_line,
    print_seed_result_line,
    print_run_end_messages,
)

from dbt.contracts.results import RunStatus
from dbt.exceptions import InternalException
from dbt.graph import ResourceTypeSelector
from dbt.logger import GLOBAL_LOGGER as logger, TextOnly
from dbt.node_types import NodeType


class SeedRunner(ModelRunner):
    def describe_node(self):
        return "seed file {}".format(self.get_node_representation())

    def before_execute(self):
        description = self.describe_node()
        print_start_line(description, self.node_index, self.num_nodes)

    def _build_run_model_result(self, model, context):
        result = super()._build_run_model_result(model, context)
        agate_result = context['load_result']('agate_table')
        result.agate_table = agate_result.table
        return result

    def compile(self, manifest):
        return self.node

    def print_result_line(self, result):
        schema_name = self.node.schema
        print_seed_result_line(result, schema_name, self.node_index,
                               self.num_nodes)


class SeedTask(RunTask):
    def defer_to_manifest(self, adapter, selected_uids):
        # seeds don't defer
        return

    def raise_on_first_error(self):
        return False

    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise InternalException(
                'manifest and graph must be set to get perform node selection'
            )
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
            logger.info("")
        logger.info(header)
        logger.info("-" * len(header))
        rand_table.print_table(max_rows=10, max_columns=None)
        with TextOnly():
            logger.info("")

    def show_tables(self, results):
        for result in results:
            if result.status != RunStatus.Error:
                self.show_table(result)
