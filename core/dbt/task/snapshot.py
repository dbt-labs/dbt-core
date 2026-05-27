import re
from typing import Optional, Type

from dbt.artifacts.schemas.results import NodeStatus
from dbt.events.types import LogSnapshotResult
from dbt.graph import ResourceTypeSelector
from dbt.node_types import NodeType
from dbt.task import group_lookup
from dbt.task.base import BaseRunner, ExecutionContext
from dbt.task.run import ModelRunner, RunTask
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtInternalError
from dbt_common.utils import cast_dict_to_dict_of_strings

# Patterns that indicate a unique key violation across different databases
_DUPLICATE_KEY_PATTERNS = re.compile(
    r"duplicate row|duplicate key|unique constraint|unique.*violation|"
    r"duplicate entry|cannot insert duplicate|violates unique|"
    r"integrity constraint.*unique|duplicate value",
    re.IGNORECASE,
)


class SnapshotRunner(ModelRunner):
    def describe_node(self) -> str:
        return "snapshot {}".format(self.get_node_representation())

    def handle_exception(self, e: Exception, ctx: ExecutionContext) -> str:
        error = super().handle_exception(e, ctx)
        if _DUPLICATE_KEY_PATTERNS.search(error):
            unique_key = getattr(self.node.config, "unique_key", None)
            unique_key_str = (
                ", ".join(unique_key) if isinstance(unique_key, list) else str(unique_key)
            )
            hint = (
                "\n\nThis error is likely because the configured unique_key"
                f" ({unique_key_str}) is not actually unique in the source"
                " data. You can verify this by running:\n\n"
                f"  select {unique_key_str}, count(*)\n"
                f"  from <your_source_table>\n"
                f"  group by {unique_key_str}\n"
                f"  having count(*) > 1\n\n"
                "See https://docs.getdbt.com/docs/build/snapshots"
                "#ensure-your-unique-key-is-really-unique"
            )
            error += hint
        return error

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
