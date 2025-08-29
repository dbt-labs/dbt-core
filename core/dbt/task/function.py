from dbt.artifacts.schemas.results import NodeStatus
from dbt.artifacts.schemas.run import RunResult
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import FunctionNode
from dbt.events.types import LogNodeResult, LogStartLine
from dbt.task import group_lookup
from dbt.task.compile import CompileRunner
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event


class FunctionRunner(CompileRunner):

    def __init__(self, config, adapter, node, node_index: int, num_nodes: int) -> None:
        super().__init__(config, adapter, node, node_index, num_nodes)

        # doing this gives us type hints for the node :D
        assert isinstance(node, FunctionNode)
        self.node = node

    def describe_node(self) -> str:
        return f"function {self.node.name}"  # TODO: add more info, similar to SeedRunner.describe_node

    def before_execute(self) -> None:
        fire_event(
            LogStartLine(
                description=self.describe_node(),
                index=self.node_index,
                total=self.num_nodes,
                node_info=self.node.node_info,
            )
        )

    def execute(self, compiled_node: FunctionNode, manifest: Manifest):
        raise NotImplementedError(
            "FunctionRunner.execute is not implemented"
        )  # TODO: add execute logic

    def after_execute(self, result: RunResult) -> None:
        pass  # TODO: add after_execute logic to print the result

    # def compile() defined on CompileRunner

    def print_result_line(self, result: RunResult) -> None:
        node = result.node
        assert isinstance(node, FunctionNode)

        group = group_lookup.get(node.unique_id)
        level = EventLevel.ERROR if result.status == NodeStatus.Error else EventLevel.INFO
        fire_event(
            LogNodeResult(  # TODO: rename to LogFunctionResult
                status=result.status,
                result_message=result.message,
                index=self.node_index,
                total=self.num_nodes,
                execution_time=result.execution_time,
                schema=self.node.schema,
                relation=node.alias,
                node_info=node.node_info,
                group=group,
            ),
            level=level,
        )
