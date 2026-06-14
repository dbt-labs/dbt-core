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

DUPLICATE_ROW_INDICATORS = (
    "duplicate row detected during dml action",
    "update/merge must match at most one source row",
    "merge statement resulted in multiple rows",
    "duplicate key value violates unique constraint",
    "ora-30926",
)

SNAPSHOT_UNIQUE_KEY_SUGGESTION = (
    "Suggestion: Ensure your unique_key column(s) are really unique. "
    "See https://docs.getdbt.com/docs/build/snapshots#ensure-your-unique-key-is-really-unique"
)




class SnapshotRunner(ModelRunner):
    def describe_node(self) -> str:
        return "snapshot {}".format(self.get_node_representation())

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

    def _extract_msg(self, exc: Exception) -> Optional[str]:
        if getattr(exc, "msg", None) and isinstance(exc.msg, str):
            return exc.msg
        if exc.args and isinstance(exc.args[0], str):
            return exc.args[0]
        return None

    def _update_exc_msg(self, exc: Exception, new_msg: str) -> None:
        if hasattr(exc, "msg") and isinstance(exc.msg, str):
            exc.msg = new_msg
        else:
            exc.args = (new_msg, *exc.args[1:])

    def _is_duplicate_row_error(self, msg: str) -> bool:
        if SNAPSHOT_UNIQUE_KEY_SUGGESTION in msg:
            return False
        msg_lower = msg.lower()
        for indicator in DUPLICATE_ROW_INDICATORS:
            if indicator in msg_lower:
                return True
        return False

    def handle_exception(self, exc: Exception, ctx) -> str:
        msg = self._extract_msg(exc)
        if msg and self._is_duplicate_row_error(msg):
            self._update_exc_msg(exc, f"{msg}\n\n{SNAPSHOT_UNIQUE_KEY_SUGGESTION}")

        return super().handle_exception(exc, ctx)


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
