from unittest import mock

import pytest

from dbt.cli.exceptions import DbtUsageException
from dbt.cli.main import dbtRunner
from dbt.exceptions import DbtProjectError
from dbt.adapters.factory import reset_adapters, FACTORY
from dbt.tests.util import read_file, write_file
from dbt.version import __version__ as dbt_version


class TestDbtRunner:
    @pytest.fixture
    def dbt(self) -> dbtRunner:
        return dbtRunner()

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models.sql": "select 1 as id",
        }

    def test_group_invalid_option(self, dbt: dbtRunner) -> None:
        res = dbt.invoke(["--invalid-option"])
        assert type(res.exception) == DbtUsageException

    def test_command_invalid_option(self, dbt: dbtRunner) -> None:
        res = dbt.invoke(["deps", "--invalid-option"])
        assert type(res.exception) == DbtUsageException

    def test_command_mutually_exclusive_option(self, dbt: dbtRunner) -> None:
        res = dbt.invoke(["--warn-error", "--warn-error-options", '{"include": "all"}', "deps"])
        assert type(res.exception) == DbtUsageException
        res = dbt.invoke(["deps", "--warn-error", "--warn-error-options", '{"include": "all"}'])
        assert type(res.exception) == DbtUsageException

    def test_invalid_command(self, dbt: dbtRunner) -> None:
        res = dbt.invoke(["invalid-command"])
        assert type(res.exception) == DbtUsageException

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
        res = dbt.invoke(
            ["run"],
            log_format="json",
            log_path="some_random_path",
            version_check=False,
            profile_name="some_random_profile_name",
            target_dir="some_random_target_dir",
        )
        assert res.result.args["log_format"] == "json"
        assert res.result.args["log_path"] == "some_random_path"
        assert res.result.args["version_check"] is False
        assert res.result.args["profile_name"] == "some_random_profile_name"
        assert res.result.args["target_dir"] == "some_random_target_dir"

    def test_invoke_kwargs_project_dir(self, project, dbt):
        res = dbt.invoke(["run"], project_dir="some_random_project_dir")
        assert type(res.exception) == DbtProjectError

        msg = "No dbt_project.yml found at expected path some_random_project_dir"
        assert msg in res.exception.msg

    def test_invoke_kwargs_profiles_dir(self, project, dbt):
        res = dbt.invoke(["run"], profiles_dir="some_random_profiles_dir")
        assert type(res.exception) == DbtProjectError
        msg = "Could not find profile named 'test'"
        assert msg in res.exception.msg

    def test_invoke_kwargs_and_flags(self, project, dbt):
        res = dbt.invoke(["--log-format=text", "run"], log_format="json")
        assert res.result.args["log_format"] == "json"

    def test_pass_in_manifest(self, project, dbt):
        result = dbt.invoke(["parse"])
        manifest = result.result

        reset_adapters()
        assert len(FACTORY.adapters) == 0
        result = dbtRunner(manifest=manifest).invoke(["run"])
        # Check that the adapters are registered again.
        assert result.success
        assert len(FACTORY.adapters) == 1


class TestDbtRunnerQueryComments:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models.sql": "select 1 as id",
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "query-comment": {
                "comment": f"comment: {dbt_version}",
                "append": True,
            }
        }

    def test_query_comment_saved_manifest(self, project, logs_dir):
        dbt = dbtRunner()
        dbt.invoke(["build", "--select", "models"])
        result = dbt.invoke(["parse"])
        write_file("", logs_dir, "dbt.log")
        # pass in manifest from parse command
        dbt = dbtRunner(result.result)
        dbt.invoke(["build", "--select", "models"])
        log_file = read_file(logs_dir, "dbt.log")
        assert f"comment: {dbt_version}" in log_file
