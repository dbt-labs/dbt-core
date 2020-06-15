import os
import shutil

from dbt.contracts.rpc import (
    RPCNoParameters, RemoteEmptyResult, RemoteMethodFlags,
)
from dbt.rpc.method import RemoteMethod
from dbt.task.deps import DepsTask


def _clean_deps(config):
    if os.path.exists(config.modules_path):
        shutil.rmtree(config.modules_path)
    os.makedirs(config.modules_path)


class RemoteDepsTask(
    RemoteMethod[RPCNoParameters, RemoteEmptyResult],
    DepsTask,
):
    METHOD_NAME = 'deps'

    def get_flags(self) -> RemoteMethodFlags:
        return (
            RemoteMethodFlags.RequiresConfigReloadBefore |
            RemoteMethodFlags.RequiresManifestReloadAfter
        )

    def set_args(self, params: RPCNoParameters):
        pass

    def handle_request(self) -> RemoteEmptyResult:
        _clean_deps(self.config)
        self.run()
        return RemoteEmptyResult([])
