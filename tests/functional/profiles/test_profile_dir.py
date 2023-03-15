import os
import pytest
import yaml

from contextlib import contextmanager
from pathlib import Path
from argparse import Namespace

import dbt.flags as flags

from dbt.tests.util import (
    run_dbt_and_capture,
    write_file,
    rm_file,
)


@pytest.fixture(scope="class")
def profiles_yml(profiles_root, dbt_profile_data):
    write_file(yaml.safe_dump(dbt_profile_data), profiles_root, "profiles.yml")
    return dbt_profile_data


@pytest.fixture(scope="class")
def profiles_home_root():
    return os.path.join(os.path.expanduser("~"), ".dbt")


@pytest.fixture(scope="class")
def profiles_env_root(tmpdir_factory):
    path = tmpdir_factory.mktemp("profile_env")
    # environment variables are lowercased for some reason in _get_flag_value_from_env within dbt.flags
    return str(path).lower()


@pytest.fixture(scope="class")
def profiles_flag_root(tmpdir_factory):
    return tmpdir_factory.mktemp("profile_flag")


@pytest.fixture(scope="class")
def profiles_project_root(project):
    return project.project_root


@pytest.fixture(scope="class")
def cwd():
    return os.getcwd()


@pytest.fixture(scope="class")
def cwd_parent(cwd):
    return os.path.dirname(cwd)


@pytest.fixture(scope="class")
def cwd_child():
    # pick any child directory of the dbt project
    return Path(os.getcwd()) / "macros"


@pytest.fixture
def write_profiles_yml(request):
    def _write_profiles_yml(profiles_dir, dbt_profile_contents):
        def cleanup():
            rm_file(Path(profiles_dir) / "profiles.yml")

        request.addfinalizer(cleanup)
        write_file(yaml.safe_dump(dbt_profile_contents), profiles_dir, "profiles.yml")

    return _write_profiles_yml


# https://gist.github.com/igniteflow/7267431?permalink_comment_id=2551951#gistcomment-2551951
@contextmanager
def environ(env):
    """Temporarily set environment variables inside the context manager and
    fully restore previous environment afterwards
    """
    original_env = {key: os.getenv(key) for key in env}
    os.environ.update(env)
    try:
        yield
    finally:
        for key, value in original_env.items():
            if value is None:
                del os.environ[key]
            else:
                os.environ[key] = value


class TestProfiles:
    def dbt_debug(self, project_dir_cli_arg=None, profiles_dir_cli_arg=None):
        # begin with no command-line args or user config (from profiles.yml)
        flags.set_from_args(Namespace(), {})
        command = ["debug"]

        if project_dir_cli_arg:
            command.extend(["--project-dir", str(project_dir_cli_arg)])

        if profiles_dir_cli_arg:
            command.extend(["--profiles-dir", str(profiles_dir_cli_arg)])

        # get the output of `dbt debug` regardless of the exit code
        return run_dbt_and_capture(command, expect_pass=None)

    @pytest.mark.parametrize(
        "project_dir_cli_arg, working_directory",
        [
            # 3 different scenarios for `--project-dir` flag and current working directory
            (None, "cwd"),  # no --project-dir flag and cwd is project directory
            (None, "cwd_child"),  # no --project-dir flag and cwd is a project subdirectory
            ("cwd", "cwd_parent"),  # use --project-dir flag and cwd is outside of it
        ],
    )
    def test_profiles(
        self,
        project_dir_cli_arg,
        working_directory,
        write_profiles_yml,
        dbt_profile_data,
        profiles_home_root,
        profiles_project_root,
        profiles_flag_root,
        profiles_env_root,
        request,
    ):
        """Verify priority order to search for profiles.yml configuration.

        Reverse priority order:
        1. HOME directory
        2. DBT_PROFILES_DIR environment variable
        3. --profiles-dir command-line argument

        Specification later in this list will take priority over earlier ones, even when both are provided.
        """

        # https://pypi.org/project/pytest-lazy-fixture/ is an alternative to using request.getfixturevalue
        if project_dir_cli_arg is not None:
            project_dir_cli_arg = request.getfixturevalue(project_dir_cli_arg)

        if working_directory is not None:
            working_directory = request.getfixturevalue(working_directory)

        # start in the specified directory
        if working_directory is not None:
            os.chdir(working_directory)
        # default case with profiles.yml in the HOME directory
        _, stdout = self.dbt_debug(project_dir_cli_arg)
        assert f"Using profiles.yml file at {profiles_home_root}" in stdout

        # set DBT_PROFILES_DIR environment variable for the remainder of the cases
        env_vars = {"DBT_PROFILES_DIR": profiles_env_root}
        with environ(env_vars):
            _, stdout = self.dbt_debug(project_dir_cli_arg)
            assert f"Using profiles.yml file at {profiles_env_root}" in stdout

            # This additional case is also within the context manager because we want to verify
            # that it takes priority even when the relevant environment variable is also set

            # set --profiles-dir on the command-line
            _, stdout = self.dbt_debug(
                project_dir_cli_arg, profiles_dir_cli_arg=profiles_flag_root
            )
            assert f"Using profiles.yml file at {profiles_flag_root}" in stdout
