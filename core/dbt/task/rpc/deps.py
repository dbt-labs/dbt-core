
from .cli import HasCLI
from dbt.contracts.rpc import RPCNoParameters, RemoteEmptyResult
from dbt.task.deps import DepsTask


class RemoteDepsTask(HasCLI[RPCNoParameters, RemoteEmptyResult], DepsTask):
    METHOD_NAME = 'deps'

    def set_args(self, params: RPCNoParameters):
        pass

    def handle_request(self) -> RemoteEmptyResult:
        self.run()
        return RemoteEmptyResult([])
