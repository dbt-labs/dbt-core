from dbt.contracts.results import RunResultsArtifact
from dbt.contracts.rpc import RemoteExecutionResult
from dbt.task.runnable import GraphRunnableTask
from dbt.rpc.method import RemoteManifestMethod, Parameters


class RPCTask(
    GraphRunnableTask,
    RemoteManifestMethod[Parameters, RemoteExecutionResult]
):
    def __init__(self, args, config, manifest):
        super().__init__(args, config)
        RemoteManifestMethod.__init__(
            self, args, config, manifest  # type: ignore
        )

    def load_manifest(self):
        # we started out with a manifest!
        pass

    def get_result(
        self, results, elapsed_time, generated_at
    ) -> RemoteExecutionResult:
        base = RunResultsArtifact.from_node_results(
            results=results,
            elapsed_time=elapsed_time,
            generated_at=generated_at,
        )
        rpc_result = RemoteExecutionResult.from_local_result(base, logs=[])
        return rpc_result
