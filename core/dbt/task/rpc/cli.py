import abc
import shlex
from typing import Type


from dbt.contracts.rpc import RPCCliParameters

from dbt.rpc.method import (
    RemoteMethod,
    RemoteManifestMethod,
    Parameters,
    Result,
)
from dbt.exceptions import InternalException

from .base import RPCTask


class HasCLI(RemoteMethod[Parameters, Result]):
    @classmethod
    def has_cli_parameters(cls):
        return True

    @abc.abstractmethod
    def handle_request(self) -> Result:
        pass


class RemoteRPCParameters(RPCTask[RPCCliParameters]):
    METHOD_NAME = 'cli_args'

    def set_args(self, params: RPCCliParameters) -> None:
        # more import cycles :(
        from dbt.main import parse_args, RPCArgumentParser
        split = shlex.split(params.cli)
        self.args = parse_args(split, RPCArgumentParser)
        cls = self.get_rpc_task_cls()
        if issubclass(cls, RemoteManifestMethod):
            self.real_task = cls(self.args, self.config, self.manifest)
        else:
            self.real_task = cls(self.args, self.config)

    def get_rpc_task_cls(self) -> Type[HasCLI]:
        # This is obnoxious, but we don't have actual access to the TaskManager
        # so instead we get to dig through all the subclasses of RPCTask
        # (recursively!) looking for a matching METHOD_NAME
        candidate: Type[HasCLI]
        for candidate in HasCLI.recursive_subclasses():
            if candidate.METHOD_NAME == self.args.rpc_method:
                return candidate
        # this shouldn't happen
        raise InternalException(
            'No matching handler found for rpc method {} (which={})'
            .format(self.args.rpc_method, self.args.which)
        )

    def load_manifest(self):
        # we started out with a manifest!
        pass

    def handle_request(self) -> Result:
        # we parsed args from the cli, so we're set on that front
        return self.real_task.handle_request()

    def interpret_results(self, results):
        if self.real_task is None:
            # I don't know what happened, but it was surely some flavor of
            # failure
            return False
        return self.real_task.interpret_results(results)
