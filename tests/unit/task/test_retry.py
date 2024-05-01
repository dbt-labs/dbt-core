from dbt.cli.types import Command
from dbt.task.retry import TASK_DICT, CMD_DICT

EXCLUDED_COMMANDS = {
    "clean",
    "debug",
    "deps",
    "freshness",
    "init",
    "list",
    "parse",
    "retry",
    "show",
    "serve",
}


def test_task_cmd_dicts():
    assert TASK_DICT.keys() == CMD_DICT.keys()


def test_exhaustive_commands():
    assert set(TASK_DICT.keys()).union(EXCLUDED_COMMANDS) == set(i.value.lower() for i in Command)
