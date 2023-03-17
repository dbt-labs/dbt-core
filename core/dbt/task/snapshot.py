from .run import ModelRunner, RunTask

from dbt.exceptions import DbtInternalError
from dbt.events.functions import fire_event
from dbt.events.base_types import EventLevel
from dbt.events.types import LogSnapshotResult
from dbt.graph import ResourceTypeSelector
from dbt.node_types import NodeType
from dbt.contracts.results import NodeStatus


class SnapshotRunner(ModelRunner):
    def describe_node(self):
        return "snapshot {}".format(self.get_node_representation())

    def print_result_line(self, result):
        model = result.node
        cfg = model.config.to_dict(omit_none=True)
        level = EventLevel.ERROR if result.status == NodeStatus.Error else EventLevel.INFO
        fire_event(
            LogSnapshotResult(
                status=result.status,
                description=self.get_node_representation(),
                cfg=cfg,
                index=self.node_index,
                total=self.num_nodes,
                execution_time=result.execution_time,
                node_info=model.node_info,
            ),
            level=level,
        )


class SnapshotTask(RunTask):
    def raise_on_first_error(self):
        return False

    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get perform node selection")
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[NodeType.Snapshot],
        )

    def get_runner_type(self, _):
        return SnapshotRunner
