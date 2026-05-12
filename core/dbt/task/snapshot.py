from typing import Optional, Type

from dbt.artifacts.schemas.results import NodeStatus
from dbt.events.types import LogSnapshotResult
from dbt.graph import ResourceTypeSelector
from dbt.node_types import NodeType
from dbt.task import group_lookup
from dbt.task.base import BaseRunner
from dbt.task.run import ModelRunner, RunTask
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtInternalError
from dbt_common.utils import cast_dict_to_dict_of_strings


class SnapshotRunner(ModelRunner):
    def describe_node(self) -> str:
        return "snapshot {}".format(self.get_node_representation())

    def execute(self, compiled_node, manifest):
        try:
            # Call the parent class (ModelRunner) execution logic
            return super().execute(compiled_node, manifest)
        except Exception as e:
            # Intercept the specific database error for snapshots
            if "Duplicate row detected during DML action" in str(e):
                hint = "\n\nHint: Ensure the unique_key column(s) are really unique."

                # dbt exceptions usually store their message in the 'msg' attribute
                if hasattr(e, "msg"):
                    e.msg += hint
                # Fallback for standard Python exception arguments
                elif hasattr(e, "args") and len(e.args) > 0 and isinstance(e.args[0], str):
                    e.args = (e.args[0] + hint,) + e.args[1:]

            # Re-raise the error so dbt's terminal logger prints it normally
            raise e

    def print_result_line(self, result):
        model = result.node
        group = group_lookup.get(model.unique_id)
        cfg = model.config.to_dict(omit_none=True)
        level = EventLevel.ERROR if result.status == NodeStatus.Error else EventLevel.INFO
        fire_event(
            LogSnapshotResult(
                status=result.status,
                description=self.get_node_representation(),
                cfg=cast_dict_to_dict_of_strings(cfg),
                index=self.node_index,
                total=self.num_nodes,
                execution_time=result.execution_time,
                node_info=model.node_info,
                result_message=result.message,
                group=group,
            ),
            level=level,
        )


class SnapshotTask(RunTask):
    def raise_on_first_error(self) -> bool:
        return False

    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get perform node selection")
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[NodeType.Snapshot],
            selectors=self.config.selectors,
        )

    def get_runner_type(self, _) -> Optional[Type[BaseRunner]]:
        return SnapshotRunner
