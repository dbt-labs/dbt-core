import os
from pathlib import Path
from unittest import mock

from dbt.flags import get_flags
from dbt.include.starter_project import PACKAGE_PATH as starter_project_directory
from dbt.tests.util import run_dbt


class TestDotEnvLoadsIntoCliFlags:
    """Verify that env vars defined in a .env file in cwd are picked up by dbt CLI flags."""

    def test_dotenv_values_flow_through_to_flags(self, project):
        # Write a .env file in the cwd (which is the project directory during functional tests)
        dotenv_path = Path(os.getcwd()) / ".env"
        dotenv_path.write_text("DBT_DEBUG=True\n")

        try:
            # Invoke dbt — it should automatically load .env and pick up DBT_DEBUG
            run_dbt(["parse"])

            flags = get_flags()
            assert flags.DEBUG is True
        finally:
            dotenv_path.unlink(missing_ok=True)
            os.environ.pop("DBT_DEBUG", None)


class TestDotEnvShellEnvTakesPrecedence:
    """Verify that shell env vars take precedence over .env file values.

    This test requires .env loading to be implemented. It sets two different
    env vars — one only in .env, one in both .env and shell (with conflicting values).
    It asserts the .env-only var is loaded AND the shell value wins for the conflict.
    """

    def test_shell_env_overrides_dotenv(self, project):
        # .env sets both DBT_DEBUG=True and a marker var
        # Shell sets DBT_DEBUG=False (should override .env)
        dotenv_path = Path(os.getcwd()) / ".env"
        dotenv_path.write_text("DBT_DEBUG=True\nDBT_DOTENV_TEST_MARKER=from_dotenv\n")

        try:
            with mock.patch.dict(os.environ, {"DBT_DEBUG": "False"}):
                run_dbt(["parse"])

                flags = get_flags()
                # .env loading must work (marker var proves it)
                assert os.environ.get("DBT_DOTENV_TEST_MARKER") == "from_dotenv"
                # But shell env var wins over .env for DBT_DEBUG
                assert flags.DEBUG is False
        finally:
            dotenv_path.unlink(missing_ok=True)
            os.environ.pop("DBT_DEBUG", None)
            os.environ.pop("DBT_DOTENV_TEST_MARKER", None)


class TestDotEnvLoadsFromCwdNotProjectRoot:
    """Verify that .env is loaded from cwd, not from the project root directory."""

    def test_dotenv_uses_cwd_not_project_root(self, project, tmp_path):
        project_root = Path(os.getcwd())

        # Put .env in project root with a value that should NOT be loaded
        project_dotenv = project_root / ".env"
        project_dotenv.write_text("DBT_DOTENV_LOCATION=project_root\n")

        # Put .env in a separate directory (our "cwd") with the correct value
        cwd_dotenv = tmp_path / ".env"
        cwd_dotenv.write_text("DBT_DOTENV_LOCATION=cwd\n")

        try:
            os.chdir(tmp_path)

            # Invoke dbt with explicit --project-dir so it finds the project
            run_dbt(["parse", "--project-dir", str(project_root)])
            assert os.environ.get("DBT_DOTENV_LOCATION") == "cwd"
        finally:
            os.chdir(project_root)
            project_dotenv.unlink(missing_ok=True)
            os.environ.pop("DBT_DOTENV_LOCATION", None)


class TestDotEnvInGitignoreTemplate:
    """Verify that the starter project .gitignore template includes .env."""

    def test_gitignore_contains_dotenv(self):
        gitignore_path = Path(starter_project_directory) / ".gitignore"
        content = gitignore_path.read_text()
        assert ".env" in content
