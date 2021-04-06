import abc
import shlex
from dbt.clients.yaml_helper import Dumper, yaml  # noqa: F401
from typing import Type, Optional


from dbt.config.utils import parse_cli_vars
from dbt.contracts.rpc import RPCCliParameters

from dbt.rpc.method import (
    RemoteMethod,
    RemoteManifestMethod,
    Parameters,
    Result,
)
from dbt.exceptions import InternalException
from dbt.parser.manifest import ManifestLoader

from .base import RPCTask


class HasCLI(RemoteMethod[Parameters, Result]):
    @classmethod
    def has_cli_parameters(cls):
        return True

    @abc.abstractmethod
    def handle_request(self) -> Result:
        pass


class RemoteRPCCli(RPCTask[RPCCliParameters]):
    METHOD_NAME = 'cli_args'

    def __init__(self, args, config, manifest):
        super().__init__(args, config, manifest)
        self.task_type: Optional[Type[RemoteMethod]] = None
        self.real_task: Optional[RemoteMethod] = None

    def set_config(self, config):
        super().set_config(config)

        if self.task_type is None:
            raise InternalException('task type not set for set_config')
        if issubclass(self.task_type, RemoteManifestMethod):
            task_type: Type[RemoteManifestMethod] = self.task_type
            self.real_task = task_type(
                self.args, self.config, self.manifest
            )
        else:
            self.real_task = self.task_type(
                self.args, self.config
            )

    def set_args(self, params: RPCCliParameters) -> None:
        # more import cycles :(
        from dbt.main import parse_args, RPCArgumentParser
        split = shlex.split(params.cli)
        self.args = parse_args(split, RPCArgumentParser)
        self.task_type = self.get_rpc_task_cls()

    def get_flags(self):
        if self.task_type is None:
            raise InternalException('task type not set for get_flags')
        # this is a kind of egregious hack from a type perspective...
        return self.task_type.get_flags(self)  # type: ignore

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
        if self.real_task is None:
            raise InternalException(
                'CLI task is in a bad state: handle_request called with no '
                'real_task set!'
            )

        # It's important to update cli_vars here, because set_config()'s
        # `self.config` is before the fork(), so it would alter the behavior of
        # future calls.

        # read any cli vars we got and use it to update cli_vars
        self.config.cli_vars.update(
            parse_cli_vars(getattr(self.args, 'vars', '{}'))
        )
        # If this changed the vars, rewrite args.vars to reflect our merged
        # vars and reload the manifest.
        dumped = yaml.safe_dump(self.config.cli_vars)
        if dumped != self.args.vars:
            self.real_task.args.vars = dumped
            if isinstance(self.real_task, RemoteManifestMethod):
                self.real_task.manifest = ManifestLoader.get_full_manifest(
                    self.config, reset=True
                )

        # we parsed args from the cli, so we're set on that front
        return self.real_task.handle_request()

    def get_selection_spec(self):
        return self.real_task.get_selection_spec()

    def get_node_selector(self):
        return self.real_task.get_node_selector()

    def interpret_results(self, results):
        if self.real_task is None:
            # I don't know what happened, but it was surely some flavor of
            # failure
            return False
        return self.real_task.interpret_results(results)
