
from dbt.runner import RunManager
from dbt.task.base_task import BaseTask
from dbt.task.base_task import RunnableTask
from dbt.node_runners import FreshnessRunner
from dbt.node_types import NodeType
from dbt.ui.printer import print_timestamped_line


class FreshnessTask(RunnableTask):
    def run(self):
        include = [
            'source:{}'.format(s)
            for s in (self.args.selected or ['*'])
        ]
        query = {
            "include": include,
            "resource_types": [NodeType.Source],
            "tags": [],
            "required": ['has_freshness'],
        }
        results = RunManager(self.config, query, FreshnessRunner).run()

        print_timestamped_line('Done.')
        return results
