from pathlib import Path

from dbt.cli.flags import Flags
from dbt.cli.types import Command as CliCommand
from dbt.cli.utils import get_runtime_config
from dbt.contracts.results import NodeStatus
from dbt.contracts.state import PreviousState
from dbt.exceptions import DbtRuntimeError
from dbt.graph import GraphQueue
from dbt.task.base import ConfiguredTask
from dbt.task.build import BuildTask
from dbt.task.compile import CompileTask
from dbt.task.freshness import FreshnessTask
from dbt.task.generate import GenerateTask
from dbt.task.run import RunTask
from dbt.task.seed import SeedTask
from dbt.task.show import ShowTask
from dbt.task.snapshot import SnapshotTask
from dbt.task.test import TestTask

RETRYABLE_STATUSES = {NodeStatus.Error, NodeStatus.Fail, NodeStatus.Skipped, NodeStatus.RuntimeErr}

TASK_DICT = {
    "build": BuildTask,
    "compile": CompileTask,
    "freshness": FreshnessTask,
    "generate": GenerateTask,
    "run": RunTask,
    "seed": SeedTask,
    "show": ShowTask,
    "snapshot": SnapshotTask,
    "test": TestTask,
}

CMD_DICT = {
    "build": CliCommand.BUILD,
    "compile": CliCommand.COMPILE,
    "freshness": CliCommand.SOURCE_FRESHNESS,
    "generate": CliCommand.DOCS_GENERATE,
    "run": CliCommand.RUN,
    "seed": CliCommand.SEED,
    "show": CliCommand.SHOW,
    "snapshot": CliCommand.SNAPSHOT,
    "test": CliCommand.TEST,
}


class RetryTask(ConfiguredTask):
    def __init__(self, args, config, manifest):
        super().__init__(args, config, manifest)

        state_path = self.args.state or self.config.target_path

        if self.args.warn_error:
            RETRYABLE_STATUSES.add(NodeStatus.Warn)

        self.previous_state = PreviousState(
            state_path=Path(state_path),
            target_path=Path(self.config.target_path),
            project_root=Path(self.config.project_root),
        )

        if not self.previous_state.results:
            raise DbtRuntimeError(
                f"Could not find previous run in '{state_path}' target directory"
            )

        self.previous_args = self.previous_state.results.args
        self.previous_command_name = self.previous_args.get("which")
        self.task_class = TASK_DICT.get(self.previous_command_name)

    def run(self):
        unique_ids = set(
            [
                result.unique_id
                for result in self.previous_state.results.results
                if result.status in RETRYABLE_STATUSES
            ]
        )

        cli_command = CMD_DICT.get(self.previous_command_name)

        if "show" in self.previous_args:
            del self.previous_args["show"]

        if "resource_types" in self.previous_args and self.previous_args["resource_types"] == []:
            del self.previous_args["resource_types"]

        retry_flags = Flags.from_dict(cli_command, self.previous_args)
        retry_config = get_runtime_config(retry_flags)

        class TaskWrapper(self.task_class):
            def get_graph_queue(self):
                new_graph = self.graph.get_subset_graph(unique_ids)
                return GraphQueue(
                    new_graph.graph,
                    self.manifest,
                    unique_ids,
                )

        task = TaskWrapper(
            retry_flags,
            retry_config,
            self.manifest,
        )

        return_value = task.run()
        return return_value

    def interpret_results(self, *args, **kwargs):
        return self.task_class.interpret_results(*args, **kwargs)
