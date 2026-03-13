import os
import pytest

from dbt.cli.dbt_runner_fs import dbtRunnerFs, dbtRunnerFsException, dbtRunnerResult
from dbt.tests.util import read_file, write_file


class TestdbtRunnerFs:
    @pytest.fixture
    def dbt(self) -> dbtRunnerFs:
        return dbtRunnerFs()

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models.sql": "select 1 as id",
        }
    
    def assert_exception(self, result: dbtRunnerResult, message: str):
        assert result.success is False
        assert type(result.exception) == dbtRunnerFsException
        assert message in result.exception.message.lower()
    
    def assert_stdout(self, result: dbtRunnerResult, stdout_match: str):
        assert result.success
        assert stdout_match in result.stdout

    def test_group_invalid_option(self, dbt: dbtRunnerFs) -> None:
        res = dbt.invoke(["--invalid-option"])

        self.assert_exception(res, "error: unexpected argument")

    def test_command_invalid_option(self, dbt: dbtRunnerFs) -> None:
        res = dbt.invoke(["deps", "--invalid-option"])

        self.assert_exception(res, "error: unexpected argument")

    def test_invalid_command(self, dbt: dbtRunnerFs) -> None:
        res = dbt.invoke(["invalid-command"])

        self.assert_exception(res, "invalid-command")

    def test_invoke_version(self, dbt: dbtRunnerFs) -> None:
        res = dbt.invoke(["--version"])

        self.assert_stdout(res, "dbt-fusion 2")
    
    @pytest.mark.skip("warn-error not supported in fusion yet")
    def test_command_mutually_exclusive_option(self, project, dbt: dbtRunnerFs) -> None:
        res = dbt.invoke(["--warn-error", "--warn-error-options", '{"error": "all"}', "deps"])
        assert type(res.exception) == dbtRunnerFsException

        res = dbt.invoke(["deps", "--warn-error", "--warn-error-options", '{"error": "all"}'])
        assert type(res.exception) == dbtRunnerFsException

        res = dbt.invoke(["compile", "--select", "models", "--inline", "select 1 as id"])
        assert type(res.exception) == dbtRunnerFsException

    def test_invoke_parse(self, project, dbt):
        res = dbt.invoke(["parse"])

        assert res.success
        assert res.exception is None

        assert res.result.manifest
        assert len(res.result.manifest.nodes) == 1
    
    # TODO: run is returning exitcode -11 inside subprocess?
    # def test_invoke_run(self, project, dbt):
    #     res = dbt.invoke(["run"])

    #     assert res.success
    #     assert res.exception is None

    #     assert res.result.run_results

    # def test_invoke_kwargs(self, project, dbt):
    #     res = dbt.invoke(
    #         ["run"],
    #         version_check=False,
    #     )

    #     assert res.result.run_results.args["version_check"] is False

    def test_invoke_kwargs_project_dir(self, project, dbt):
        res = dbt.invoke(["run"], project_dir="some_random_project_dir")

        self.assert_exception(res, "no such file or directory")

    def test_invoke_kwargs_profiles_dir(self, project, dbt):
        res = dbt.invoke(["run"], profiles_dir="some_random_profiles_dir")

        self.assert_exception(res, "no profiles.yml found at `some_random_profiles_dir/profiles.yml")

    # TODO
    # def test_invoke_kwargs_and_flags(self, project, dbt):
    #     res = dbt.invoke(["--log-format=text", "run"], log_format="json")
    #     assert res.result.run_results.args["log_format"] == "json"

    def test_pass_in_args_variable(self, dbt):
        args = ["--no-version-check"]
        args_before = args.copy()
        dbt.invoke(args)
        assert args == args_before

    def test_directory_does_not_change(self, project, dbt: dbtRunnerFs) -> None:
        project_dir = os.getcwd()  # The directory where dbt_project.yml exists.
        os.chdir("../")
        cmd_execution_dir = os.getcwd()  # The directory where dbt command will be run

        commands = ["init", "deps", "clean"]
        for command in commands:
            args = [command, "--project-dir", project_dir]
            if command == "init":
                args.append("--skip-profile-setup")
            res = dbt.invoke(args)
            after_dir = os.getcwd()
            assert res.success is True
            assert cmd_execution_dir == after_dir


# class TestDbtRunnerQueryComments:
#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "models.sql": "select 1 as id",
#         }

#     @pytest.fixture(scope="class")
#     def project_config_update(self):
#         return {
#             "query-comment": {
#                 "comment": f"comment: test",
#                 "append": True,
#             }
#         }

#     def test_query_comment_saved_manifest(self, project, logs_dir):
#         dbt = dbtRunnerFs()
#         dbt.invoke(["build", "--select", "models"])
#         result = dbt.invoke(["parse"])
#         write_file("", logs_dir, "dbt.log")
#         # pass in manifest from parse command
#         dbt = dbtRunnerFs(result.result)
#         dbt.invoke(["build", "--select", "models"])
#         log_file = read_file(logs_dir, "dbt.log")
#         assert f"comment: test" in log_file

