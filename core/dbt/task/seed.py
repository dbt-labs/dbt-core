import random
from typing import List, Dict, Any

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_runners import SeedRunner
from dbt.node_types import NodeType
from dbt.task.run import RunTask
from dbt.task.runnable import RemoteCallable
import dbt.ui.printer


class SeedTask(RunTask):
    def raise_on_first_error(self):
        return False

    def build_query(self):
        return {
            "include": ["*"],
            "exclude": [],
            "resource_types": [NodeType.Seed],
        }

    def get_runner_type(self):
        return SeedRunner

    def task_end_messages(self, results):
        if self.args.show:
            self.show_tables(results)

        dbt.ui.printer.print_run_end_messages(results)

    def show_table(self, result):
        table = result.agate_table
        rand_table = table.order_by(lambda x: random.random())

        schema = result.node.schema
        alias = result.node.alias

        header = "Random sample of table: {}.{}".format(schema, alias)
        logger.info("")
        logger.info(header)
        logger.info("-" * len(header))
        rand_table.print_table(max_rows=10, max_columns=None)
        logger.info("")

    def show_tables(self, results):
        for result in results:
            if result.error is None:
                self.show_table(result)


class RemoteSeedProjectTask(SeedTask, RemoteCallable):
    METHOD_NAME = 'seed_project'

    def __init__(self, args, config, manifest):
        super().__init__(args, config)
        self.manifest = manifest.deepcopy(config=config)

    def load_manifest(self):
        # we started out with a manifest!
        pass

    def handle_request(self, show: bool = False) -> Dict[str, List[Any]]:
        self.args.show = show

        results = self.run()
        return {'results': [r.to_dict() for r in results]}
