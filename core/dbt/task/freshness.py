import os
from dbt.task.runnable import BaseRunnableTask
from dbt.node_runners import FreshnessRunner
from dbt.node_types import NodeType
from dbt.ui.printer import print_timestamped_line, print_run_result_error
from dbt.contracts.results import FreshnessExecutionResult

RESULT_FILE_NAME = 'sources.json'


class FreshnessTask(BaseRunnableTask):
    def result_path(self):
        if self.args.output:
            return os.path.realpath(self.args.output)
        else:
            return os.path.join(self.config.target_path, RESULT_FILE_NAME)

    def raise_on_first_error(self):
        return False

    def build_query(self):
        include = [
            'source:{}'.format(s)
            for s in (self.args.selected or ['*'])
        ]
        return {
            "include": include,
            "resource_types": [NodeType.Source],
            "tags": [],
            "required": ['has_freshness'],
        }

    def get_runner_type(self):
        return FreshnessRunner

    def get_result(self, results, elapsed_time, generated_at):
        return FreshnessExecutionResult(
            elapsed_time=elapsed_time,
            generated_at=generated_at,
            results=results
        )

    def task_end_messages(self, results):
        for result in results:
            if result.error is not None:
                print_run_result_error(result)

        print_timestamped_line('Done.')
