from datetime import datetime
from typing import List, Optional, Union


from dbt.contracts.rpc import (
    RPCCompileParameters,
    RPCDocsGenerateParameters,
    RPCRunOperationParameters,
    RPCSeedParameters,
    RPCTestParameters,
    RemoteCatalogResults,
    RemoteExecutionResult,
    RemoteRunOperationResult,
    RPCSnapshotParameters,
    RPCSourceFreshnessParameters,
)
from dbt.rpc.method import (
    Parameters,
)
from dbt.task.compile import CompileTask
from dbt.task.freshness import FreshnessTask
from dbt.task.generate import GenerateTask
from dbt.task.run import RunTask
from dbt.task.run_operation import RunOperationTask
from dbt.task.seed import SeedTask
from dbt.task.snapshot import SnapshotTask
from dbt.task.test import TestTask

from .base import RPCTask
from .cli import HasCLI


class RPCCommandTask(
    RPCTask[Parameters],
    HasCLI[Parameters, RemoteExecutionResult],
):
    @staticmethod
    def _listify(
        value: Optional[Union[str, List[str]]]
    ) -> Optional[List[str]]:
        if value is None:
            return None
        elif isinstance(value, str):
            return [value]
        else:
            return value

    def handle_request(self) -> RemoteExecutionResult:
        return self.run()


class RemoteCompileProjectTask(
    RPCCommandTask[RPCCompileParameters], CompileTask
):
    METHOD_NAME = 'compile'

    def set_args(self, params: RPCCompileParameters) -> None:
        self.args.models = self._listify(params.models)
        self.args.exclude = self._listify(params.exclude)
        if params.threads is not None:
            self.args.threads = params.threads


class RemoteRunProjectTask(RPCCommandTask[RPCCompileParameters], RunTask):
    METHOD_NAME = 'run'

    def set_args(self, params: RPCCompileParameters) -> None:
        self.args.models = self._listify(params.models)
        self.args.exclude = self._listify(params.exclude)
        if params.threads is not None:
            self.args.threads = params.threads


class RemoteSeedProjectTask(RPCCommandTask[RPCSeedParameters], SeedTask):
    METHOD_NAME = 'seed'

    def set_args(self, params: RPCSeedParameters) -> None:
        # select has an argparse `dest` value of `models`.
        self.args.models = self._listify(params.select)
        self.args.exclude = self._listify(params.exclude)
        if params.threads is not None:
            self.args.threads = params.threads
        self.args.show = params.show


class RemoteTestProjectTask(RPCCommandTask[RPCTestParameters], TestTask):
    METHOD_NAME = 'test'

    def set_args(self, params: RPCTestParameters) -> None:
        self.args.models = self._listify(params.models)
        self.args.exclude = self._listify(params.exclude)
        self.args.data = params.data
        self.args.schema = params.schema
        if params.threads is not None:
            self.args.threads = params.threads


class RemoteDocsGenerateProjectTask(
    RPCCommandTask[RPCDocsGenerateParameters],
    GenerateTask,
):
    METHOD_NAME = 'docs.generate'

    def set_args(self, params: RPCDocsGenerateParameters) -> None:
        self.args.models = None
        self.args.exclude = None
        self.args.compile = params.compile

    def get_catalog_results(
        self, nodes, generated_at, compile_results
    ) -> RemoteCatalogResults:
        return RemoteCatalogResults(
            nodes=nodes,
            generated_at=datetime.utcnow(),
            _compile_results=compile_results,
            logs=[],
        )


class RemoteRunOperationTask(
    RPCTask[RPCRunOperationParameters],
    HasCLI[RPCRunOperationParameters, RemoteRunOperationResult],
    RunOperationTask,
):
    METHOD_NAME = 'run-operation'

    def set_args(self, params: RPCRunOperationParameters) -> None:
        self.args.macro = params.macro
        self.args.args = params.args

    def _get_kwargs(self):
        if isinstance(self.args.args, dict):
            return self.args.args
        else:
            return RunOperationTask._get_kwargs(self)

    def _runtime_initialize(self):
        return RunOperationTask._runtime_initialize(self)

    def handle_request(self) -> RemoteRunOperationResult:
        success, _ = RunOperationTask.run(self)
        result = RemoteRunOperationResult(logs=[], success=success)
        return result

    def interpret_results(self, results):
        return results.success


class RemoteSnapshotTask(RPCCommandTask[RPCSnapshotParameters], SnapshotTask):
    METHOD_NAME = 'snapshot'

    def set_args(self, params: RPCSnapshotParameters) -> None:
        # select has an argparse `dest` value of `models`.
        self.args.models = self._listify(params.select)
        self.args.exclude = self._listify(params.exclude)
        if params.threads is not None:
            self.args.threads = params.threads


class RemoteSourceFreshnessTask(
    RPCCommandTask[RPCSourceFreshnessParameters],
    FreshnessTask
):
    METHOD_NAME = 'snapshot-freshness'

    def set_args(self, params: RPCSourceFreshnessParameters) -> None:
        self.args.selected = self._listify(params.select)
        if params.threads is not None:
            self.args.threads = params.threads
        self.args.output = None
