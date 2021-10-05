import os
import shutil

from dbt.contracts.rpc import (
    RPCDepsParameters, RemoteDepsResult, RemoteMethodFlags,
)
from dbt.rpc.method import RemoteMethod
from dbt.task.deps import DepsTask


def _clean_deps(config):
    if os.path.exists(config.packages_install_path):
        shutil.rmtree(config.packages_install_path)
    os.makedirs(config.packages_install_path)


class RemoteDepsTask(
    RemoteMethod[RPCDepsParameters, RemoteDepsResult],
    DepsTask,
):
    METHOD_NAME = 'deps'

    def get_flags(self) -> RemoteMethodFlags:
        return (
            RemoteMethodFlags.RequiresConfigReloadBefore |
            RemoteMethodFlags.RequiresManifestReloadAfter
        )

    def set_args(self, params: RPCDepsParameters):
        pass

    def handle_request(self) -> RemoteDepsResult:
        _clean_deps(self.config)
        self.run()
        return RemoteDepsResult([])
