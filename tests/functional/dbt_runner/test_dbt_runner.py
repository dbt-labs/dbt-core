from unittest import mock

import pytest

from dbt.cli.main import dbtRunner, dbtUsageException
from dbt.exceptions import DbtProjectError


class TestDbtRunner:
    @pytest.fixture
    def dbt(self) -> dbtRunner:
        return dbtRunner()

    def test_group_invalid_option(self, dbt: dbtRunner) -> None:
        with pytest.raises(dbtUsageException):
            dbt.invoke(["--invalid-option"])

    def test_command_invalid_option(self, dbt: dbtRunner) -> None:
        with pytest.raises(dbtUsageException):
            dbt.invoke(["deps", "--invalid-option"])

    def test_command_mutually_exclusive_option(self, dbt: dbtRunner) -> None:
        with pytest.raises(dbtUsageException):
            dbt.invoke(["--warn-error", "--warn-error-options", '{"include": "all"}', "deps"])

    def test_invalid_command(self, dbt: dbtRunner) -> None:
        with pytest.raises(dbtUsageException):
            dbt.invoke(["invalid-command"])

    def test_invoke_version(self, dbt: dbtRunner) -> None:
        dbt.invoke(["--version"])

    def test_callbacks(self) -> None:
        mock_callback = mock.MagicMock()
        dbt = dbtRunner(callbacks=[mock_callback])
        # the `debug` command is one of the few commands wherein you don't need
        # to have a project to run it and it will emit events
        dbt.invoke(["debug"])
        mock_callback.assert_called()

    def test_invoke_kwargs(self, project, dbt):
        results, success = dbt.invoke(
            ["run"],
            log_format="json",
            log_path="some_random_path",
            version_check=False,
            profile_name="some_random_profile_name",
            target_dir="some_random_target_dir",
        )
        assert results.args["log_format"] == "json"
        assert results.args["log_path"] == "some_random_path"
        assert results.args["version_check"] is False
        assert results.args["profile_name"] == "some_random_profile_name"
        assert results.args["target_dir"] == "some_random_target_dir"

    def test_invoke_kwargs_project_dir(self, project, dbt):
        with pytest.raises(
            DbtProjectError,
            match="No dbt_project.yml found at expected path some_random_project_dir",
        ):
            dbt.invoke(["run"], project_dir="some_random_project_dir")

    def test_invoke_kwargs_profiles_dir(self, project, dbt):
        with pytest.raises(DbtProjectError, match="Could not find profile named 'test'"):
            dbt.invoke(["run"], profiles_dir="some_random_profiles_dir")

    def test_invoke_kwargs_and_flags(self, project, dbt):
        results, success = dbt.invoke(["--log-format=text", "run"], log_format="json")
        assert results.args["log_format"] == "json"
