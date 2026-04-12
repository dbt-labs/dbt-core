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
from dbt_common.exceptions import DbtInternalError, DbtRuntimeError
from dbt_common.utils import cast_dict_to_dict_of_strings

# Common database error patterns that indicate duplicate key violations
# during snapshot MERGE operations. These are checked case-insensitively.
_DUPLICATE_KEY_ERROR_PATTERNS = [
    "duplicate row",
    "duplicate key",
    "unique constraint",
    "cardinality violation",
    "matched more than one",
    "merge keys are not unique",
]


def _is_duplicate_key_error(error_message: str) -> bool:
    """Check if a database error message indicates a duplicate key violation."""
    lower_msg = error_message.lower()
    return any(pattern in lower_msg for pattern in _DUPLICATE_KEY_ERROR_PATTERNS)


def _format_unique_key(unique_key) -> str:
    """Format the unique_key config for display, handling both string and list forms."""
    if isinstance(unique_key, list):
        return ", ".join(str(k) for k in unique_key)
    return str(unique_key)


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

    def handle_exception(self, e, ctx):
        """Override to provide better error messages for duplicate key violations.

        When a snapshot MERGE fails due to duplicate values in the unique_key
        column(s), the database error is typically cryptic (e.g., "Duplicate row
        detected during DML action"). This override detects such errors and
        re-raises with a message that includes the snapshot name, unique_key
        column(s), and actionable guidance.
        """
        error_message = str(e)

        if _is_duplicate_key_error(error_message):
            node = self.node
            snapshot_name = node.name if hasattr(node, "name") else "unknown"
            unique_key = (
                node.config.unique_key
                if hasattr(node, "config") and hasattr(node.config, "unique_key")
                else "unknown"
            )
            file_path = (
                node.original_file_path
                if hasattr(node, "original_file_path")
                else "unknown"
            )

            raise DbtRuntimeError(
                f'Snapshot "{snapshot_name}" ({file_path}) failed due to '
                f"duplicate values in unique_key: {_format_unique_key(unique_key)}.\n\n"
                f"The unique_key column(s) must uniquely identify each row in the "
                f"source query. When duplicates exist, the snapshot MERGE statement "
                f"cannot determine which source row to use for each target row.\n\n"
                f"To fix this:\n"
                f"  1. Check your source query for duplicate {_format_unique_key(unique_key)} values\n"
                f"  2. Add a filter or deduplication step to ensure uniqueness\n"
                f"  3. Consider using a composite unique_key if a single column is not sufficient\n\n"
                f"Original error: {error_message}"
            ) from e

        # For all other errors, use default handling
        return super().handle_exception(e, ctx)


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
