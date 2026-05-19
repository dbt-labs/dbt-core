from unittest.mock import patch

from dbt.task.run import ModelRunner
from dbt.task.snapshot import (
    SNAPSHOT_UNIQUE_KEY_HINT,
    SnapshotRunner,
    _add_snapshot_unique_key_hint,
)


def test_add_snapshot_unique_key_hint_for_duplicate_row_dml_error():
    message = "Database Error\n  Duplicate row detected during DML action"

    updated_message = _add_snapshot_unique_key_hint(message)

    assert message in updated_message
    assert SNAPSHOT_UNIQUE_KEY_HINT in updated_message


def test_add_snapshot_unique_key_hint_only_once():
    message = (
        "Database Error\n"
        "  Duplicate row detected during DML action\n\n"
        f"{SNAPSHOT_UNIQUE_KEY_HINT}"
    )

    assert _add_snapshot_unique_key_hint(message) == message


def test_add_snapshot_unique_key_hint_ignores_other_errors():
    message = "Database Error\n  relation does not exist"

    assert _add_snapshot_unique_key_hint(message) == message


def test_snapshot_runner_adds_unique_key_hint_to_duplicate_row_error():
    message = "Database Error\n  Duplicate row detected during DML action"
    runner = object.__new__(SnapshotRunner)

    with patch.object(ModelRunner, "handle_exception", return_value=message):
        updated_message = runner.handle_exception(Exception("database failed"), object())

    assert message in updated_message
    assert SNAPSHOT_UNIQUE_KEY_HINT in updated_message
