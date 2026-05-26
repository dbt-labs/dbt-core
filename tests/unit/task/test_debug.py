from unittest import mock

import dbt_common.exceptions
from dbt.artifacts.schemas.results import RunStatus
from dbt.task.debug import DebugTask


def test_test_git_includes_command_output_for_nonzero_return_codes():
    task = object.__new__(DebugTask)
    error = dbt_common.exceptions.CommandResultError(
        cwd="/tmp",
        cmd=["/usr/bin/git", "--help"],
        returncode=1,
        stdout=b"",
        stderr=(
            b"xcrun: error: invalid active developer path "
            b"(/Library/Developer/CommandLineTools)"
        ),
    )

    with mock.patch("dbt.task.debug.dbt_common.clients.system.run_cmd", side_effect=error):
        result = task.test_git()

    assert result.run_status == RunStatus.Error
    assert "Got a non-zero returncode running" in result.summary_message
    assert "Error from git --help: xcrun: error: invalid active developer path" in (
        result.summary_message
    )
    assert "Make sure that `git` is installed in your shell" in result.summary_message


def test_test_git_handles_missing_executable_errors():
    task = object.__new__(DebugTask)
    error = dbt_common.exceptions.ExecutableError(
        cwd="/tmp",
        cmd=["git", "--help"],
        msg="No such file or directory",
    )

    with mock.patch("dbt.task.debug.dbt_common.clients.system.run_cmd", side_effect=error):
        result = task.test_git()

    assert result.run_status == RunStatus.Error
    assert 'No such file or directory: "git"' in result.summary_message
    assert "Make sure that `git` is installed in your shell" in result.summary_message
