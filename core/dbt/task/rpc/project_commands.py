from datetime import datetime
from typing import List, Optional, Union


from dbt.contracts.rpc import (
    RPCCompileParameters,
    RPCDocsGenerateParameters,
    RPCSeedParameters,
    RPCTestParameters,
    RemoteCatalogResults,
    RemoteExecutionResult,
)
from dbt.rpc.method import (
    Parameters,
)
from dbt.task.compile import CompileTask
from dbt.task.generate import GenerateTask
from dbt.task.run import RunTask
from dbt.task.seed import SeedTask
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


class RemoteRunProjectTask(RPCCommandTask[RPCCompileParameters], RunTask):
    METHOD_NAME = 'run'

    def set_args(self, params: RPCCompileParameters) -> None:
        self.args.models = self._listify(params.models)
        self.args.exclude = self._listify(params.exclude)


class RemoteSeedProjectTask(RPCCommandTask[RPCSeedParameters], SeedTask):
    METHOD_NAME = 'seed'

    def set_args(self, params: RPCSeedParameters) -> None:
        self.args.show = params.show


class RemoteTestProjectTask(RPCCommandTask[RPCTestParameters], TestTask):
    METHOD_NAME = 'test'

    def set_args(self, params: RPCTestParameters) -> None:
        self.args.models = self._listify(params.models)
        self.args.exclude = self._listify(params.exclude)
        self.args.data = params.data
        self.args.schema = params.schema


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
