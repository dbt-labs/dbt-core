
from .cli import HasCLI
from dbt.contracts.rpc import (
    RPCNoParameters, RemoteEmptyResult, RemoteMethodFlags,
)
from dbt.task.clean import CleanTask
from dbt.task.deps import DepsTask


class RemoteDepsTask(HasCLI[RPCNoParameters, RemoteEmptyResult], DepsTask):
    METHOD_NAME = 'deps'

    def get_flags(self) -> RemoteMethodFlags:
        return (
            RemoteMethodFlags.RequiresConfigReloadBefore |
            RemoteMethodFlags.RequiresManifestReloadAfter
        )

    def set_args(self, params: RPCNoParameters):
        pass

    def handle_request(self) -> RemoteEmptyResult:
        CleanTask(self.args, self.config).run()
        self.run()
        return RemoteEmptyResult([])
