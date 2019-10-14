from dbt.contracts.rpc import RemoteExecutionResult
from dbt.task.runnable import GraphRunnableTask
from dbt.rpc.method import RemoteMethod, Parameters


class RPCTask(
    GraphRunnableTask,
    RemoteMethod[Parameters, RemoteExecutionResult]
):
    def __init__(self, args, config, manifest):
        super().__init__(args, config)
        RemoteMethod.__init__(self, args, config, manifest)

    def get_result(
        self, results, elapsed_time, generated_at
    ) -> RemoteExecutionResult:
        return RemoteExecutionResult(
            results=results,
            elapsed_time=elapsed_time,
            generated_at=generated_at,
            logs=[],
        )
