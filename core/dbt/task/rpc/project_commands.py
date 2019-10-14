from datetime import datetime
import shlex
from typing import Type, List, Optional, Union


from dbt.contracts.rpc import (
    RPCCliParameters,
    RPCCompileParameters,
    RPCDocsGenerateParameters,
    RPCSeedParameters,
    RPCTestParameters,
    RemoteCatalogResults,
    RemoteExecutionResult,
)
from dbt.exceptions import InternalException
from dbt.task.compile import CompileTask
from dbt.task.generate import GenerateTask
from dbt.task.run import RunTask
from dbt.task.seed import SeedTask
from dbt.task.test import TestTask

from .base import RPCTask, Parameters


class RPCCommandTask(RPCTask[Parameters]):
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

    def load_manifest(self):
        # we started out with a manifest!
        pass

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


class RemoteRPCParameters(RPCCommandTask[RPCCliParameters]):
    METHOD_NAME = 'cli_args'

    def set_args(self, params: RPCCliParameters) -> None:
        # more import cycles :(
        from dbt.main import parse_args, RPCArgumentParser
        split = shlex.split(params.cli)
        self.args = parse_args(split, RPCArgumentParser)

    def get_rpc_task_cls(self) -> Type[RPCCommandTask]:
        # This is obnoxious, but we don't have actual access to the TaskManager
        # so instead we get to dig through all the subclasses of RPCTask
        # (recursively!) looking for a matching METHOD_NAME
        candidate: Type[RPCCommandTask]
        for candidate in RPCCommandTask.recursive_subclasses():
            if candidate.METHOD_NAME == self.args.rpc_method:
                return candidate
        # this shouldn't happen
        raise InternalException(
            'No matching handler found for rpc method {} (which={})'
            .format(self.args.rpc_method, self.args.which)
        )

    def handle_request(self) -> RemoteExecutionResult:
        cls = self.get_rpc_task_cls()
        # we parsed args from the cli, so we're set on that front
        task = cls(self.args, self.config, self.manifest)
        return task.handle_request()
