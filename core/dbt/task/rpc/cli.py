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

    def load_manifest(self):
        # we started out with a manifest!
        pass

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

    def handle_request(self) -> Result:
        cls = self.get_rpc_task_cls()
        # we parsed args from the cli, so we're set on that front
        if issubclass(cls, RemoteManifestMethod):
            task = cls(self.args, self.config, self.manifest)
        else:
            task = cls(self.args, self.config)
        return task.handle_request()
