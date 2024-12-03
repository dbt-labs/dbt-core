import threading

from dbt.artifacts.schemas.results import RunStatus
from dbt.artifacts.schemas.run import RunResult
from dbt.contracts.graph.manifest import Manifest
from dbt.events.types import LogNodeNoOpResult
from dbt.task.base import BaseRunner
from dbt_common.events.functions import fire_event


class NoOpRunner(BaseRunner):
    @property
    def description(self) -> str:
        raise NotImplementedError("description not implemented")

    def before_execute(self) -> None:
        pass

    def compile(self, manifest: Manifest):
        return self.node

    def after_execute(self, result) -> None:
        fire_event(
            LogNodeNoOpResult(
                description=self.description,
                index=self.node_index,
                total=self.num_nodes,
                node_info=self.node.node_info,
            )
        )

    def execute(self, compiled_node, manifest):
        # no-op
        return RunResult(
            node=compiled_node,
            status=RunStatus.NoOp,
            timing=[],
            thread_id=threading.current_thread().name,
            execution_time=0,
            message="NO-OP",
            adapter_response={},
            failures=0,
            batch_results=None,
            agate_table=None,
        )
