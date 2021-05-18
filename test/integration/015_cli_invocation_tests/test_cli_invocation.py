from test.integration.base import DBTIntegrationTest, use_profile
import contextlib
import os
import shutil
import pytest
import tempfile
import yaml
from typing import Dict


@contextlib.contextmanager
def change_working_directory(directory: str) -> str:
    """
    Context manager for changing the working directory.

    Parameters
    ----------
    directory : str
        The directory to which the working directory should be changed.

    Yields
    ------
    out : str
        The new working directory.
    """
    current_working_directory = os.getcwd()
    os.chdir(directory)
    try:
        yield directory
    finally:
        os.chdir(current_working_directory)


@contextlib.contextmanager
def temporary_working_directory() -> str:
    """
    Create a temporary working directory.

    Returns
    -------
    out : str
        The temporary working directory.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        with change_working_directory(tmpdir):
            yield tmpdir


def get_custom_profiles_config(database_host, custom_schema):
    return {
        "config": {
            "send_anonymous_usage_stats": False
        },
        "test": {
            "outputs": {
                "default": {
                    "type": "postgres",
                    "threads": 1,
                    "host": database_host,
                    "port": 5432,
                    "user": "root",
                    "pass": "password",
                    "dbname": "dbt",
                    "schema": custom_schema
                },
            },
            "target": "default",
        }
    }


def create_directory_with_custom_profiles(
    directory: str,
    profiles: Dict
) -> None:
    """
    Create directory with profiles.yml.

    Parameters
    ----------
    directory : str
        The directory in which a profiles file is created.
    profiles : Dict
        The profiles to put into the profiles.yml
    """
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(f"{directory}/profiles.yml", "w") as f:
        yaml.safe_dump(profiles, f, default_flow_style=True)


class ModelCopyingIntegrationTest(DBTIntegrationTest):

    def _symlink_test_folders(self):
        # dbt's normal symlink behavior breaks this test, so special-case it
        for entry in os.listdir(self.test_original_source_path):
            src = os.path.join(self.test_original_source_path, entry)
            tst = os.path.join(self.test_root_dir, entry)
            if entry == 'models':
                shutil.copytree(src, tst)
            elif entry == 'local_dependency':
                continue
            elif os.path.isdir(entry) or entry.endswith('.sql'):
                os.symlink(src, tst)

    @property
    def packages_config(self):
        path = os.path.join(self.test_original_source_path, 'local_dependency')
        return {
            'packages': [{
                'local': path,
            }],
        }


class TestCLIInvocation(ModelCopyingIntegrationTest):

    def setUp(self):
        super().setUp()
        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "test_cli_invocation_015"

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test_postgres_toplevel_dbt_run(self):
        self.run_dbt(['deps'])
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 1)
        self.assertTablesEqual("seed", "model")

    @use_profile('postgres')
    def test_postgres_subdir_dbt_run(self):
        os.chdir(os.path.join(self.models, "subdir1"))
        self.run_dbt(['deps'])

        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 1)
        self.assertTablesEqual("seed", "model")


class TestCLIInvocationWithProfilesDir(ModelCopyingIntegrationTest):

    def setUp(self):
        super().setUp()

        self.run_sql(f"DROP SCHEMA IF EXISTS {self.custom_schema} CASCADE;")
        self.run_sql(f"CREATE SCHEMA {self.custom_schema};")

        profiles = get_custom_profiles_config(
            self.database_host, self.custom_schema)
        create_directory_with_custom_profiles(
            "./dbt-profile", profiles)

        self.run_sql_file("seed_custom.sql")

    def tearDown(self):
        self.run_sql(f"DROP SCHEMA IF EXISTS {self.custom_schema} CASCADE;")
        super().tearDown()

    @property
    def schema(self):
        return "test_cli_invocation_015"

    @property
    def custom_schema(self):
        return "{}_custom".format(self.unique_schema())

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test_postgres_toplevel_dbt_run_with_profile_dir_arg(self):
        self.run_dbt(['deps'])
        results = self.run_dbt(['run', '--profiles-dir', 'dbt-profile'], profiles_dir=False)
        self.assertEqual(len(results), 1)

        actual = self.run_sql("select id from {}.model".format(self.custom_schema), fetch='one')

        expected = (1, )
        self.assertEqual(actual, expected)

        res = self.run_dbt(['test', '--profiles-dir', 'dbt-profile'], profiles_dir=False)

        # make sure the test runs against `custom_schema`
        for test_result in res:
            self.assertTrue(self.custom_schema, test_result.node.compiled_sql)


class TestCLIInvocationWithProjectDir(ModelCopyingIntegrationTest):

    @property
    def schema(self):
        return "test_cli_invocation_015"

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test_postgres_dbt_commands_with_cwd_as_project_dir(self):
        self._run_simple_dbt_commands(os.getcwd())

    @use_profile('postgres')
    def test_postgres_dbt_commands_with_randomdir_as_project_dir(self):
        workdir = self.test_root_dir
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            self._run_simple_dbt_commands(workdir)
            os.chdir(workdir)

    @use_profile('postgres')
    def test_postgres_dbt_commands_with_relative_dir_as_project_dir(self):
        workdir = self.test_root_dir
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            self._run_simple_dbt_commands(os.path.relpath(workdir, tmpdir))
            os.chdir(workdir)

    def _run_simple_dbt_commands(self, project_dir):
        self.run_dbt(['deps', '--project-dir', project_dir])
        self.run_dbt(['seed', '--project-dir', project_dir])
        self.run_dbt(['run', '--project-dir', project_dir])
        self.run_dbt(['test', '--project-dir', project_dir])
        self.run_dbt(['parse', '--project-dir', project_dir])
        self.run_dbt(['clean', '--project-dir', project_dir])
        # In case of 'dbt clean' also test that the clean-targets directories were deleted.
        for target in self.config.clean_targets:
            assert not os.path.isdir(target)


class TestCLIInvocationWithProfilesAndProjectDir(ModelCopyingIntegrationTest):

    @property
    def schema(self):
        return "test_cli_invocation_015"

    @property
    def models(self):
        return "models"

    @property
    def custom_schema(self):
        return "{}_custom".format(self.unique_schema())

    def _test_postgres_sub_command_with_profiles_separate_from_project_dir(
        self,
        dbt_sub_command: str
    ):
        """
        Test if a sub command runs well when a profiles dir is separate from a
        project dir.

        """
        profiles_dir = "./tmp-profile"
        workdir = os.getcwd()
        with temporary_working_directory() as tmpdir:

            profiles = get_custom_profiles_config(
                self.database_host, self.custom_schema)
            create_directory_with_custom_profiles(profiles_dir, profiles)

            project_dir = os.path.relpath(workdir, os.getcwd())
            if os.path.exists(f"{project_dir}/profiles.yml"):
                os.remove(f"{project_dir}/profiles.yml")

            other_args = [
                dbt_sub_command, "--profiles-dir", profiles_dir, "--project-dir", project_dir
            ]
            self.run_dbt(other_args, profiles_dir=False)

    @use_profile("postgres")
    def test_postgres_deps_with_profiles_separate_from_project_dir(self):
        self._test_postgres_sub_command_with_profiles_separate_from_project_dir("deps")

    @use_profile("postgres")
    def test_postgres_run_with_profiles_separate_from_project_dir(self):
        self._test_postgres_sub_command_with_profiles_separate_from_project_dir("deps")
        self._test_postgres_sub_command_with_profiles_separate_from_project_dir("run")

    @use_profile("postgres")
    def test_postgres_test_with_profiles_separate_from_project_dir(self):
        self._test_postgres_sub_command_with_profiles_separate_from_project_dir("deps")
        self._test_postgres_sub_command_with_profiles_separate_from_project_dir("run")
        self._test_postgres_sub_command_with_profiles_separate_from_project_dir("test")

    @use_profile("postgres")
    def test_postgres_debug_with_profiles_separate_from_project_dir(self):
        self._test_postgres_sub_command_with_profiles_separate_from_project_dir("debug")
