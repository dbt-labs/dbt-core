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


def _is_duplicate_row_error(message: str) -> bool:
    lowered = message.lower()
    return any(indicator in lowered for indicator in DUPLICATE_ROW_INDICATORS)


def _append_unique_key_suggestion(message: str) -> str:
    if SNAPSHOT_UNIQUE_KEY_SUGGESTION in message:
        return message

    return f"{message}\n\n{SNAPSHOT_UNIQUE_KEY_SUGGESTION}"


def _get_exception_message(exc: Exception) -> str | None:
    if hasattr(exc, "msg") and isinstance(exc.msg, str):
        return exc.msg
    if exc.args and isinstance(exc.args[0], str):
        return exc.args[0]
    return None


def _set_exception_message(exc: Exception, message: str) -> None:
    if hasattr(exc, "msg") and isinstance(exc.msg, str):
        exc.msg = message
        return
    exc.args = (message, *exc.args[1:])


def _add_snapshot_unique_key_suggestion(exc: Exception) -> None:
    message = _get_exception_message(exc)
    if message is None or not _is_duplicate_row_error(message):
        return
    _set_exception_message(exc, _append_unique_key_suggestion(message))


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

    def handle_exception(self, exc: Exception, ctx) -> str:
        _add_snapshot_unique_key_suggestion(exc)
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
