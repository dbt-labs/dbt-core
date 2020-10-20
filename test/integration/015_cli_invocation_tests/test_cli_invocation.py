from test.integration.base import DBTIntegrationTest, use_profile
import os
import shutil
import tempfile
import yaml


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

        # the test framework will remove this in teardown for us.
        if not os.path.exists('./dbt-profile'):
            os.makedirs('./dbt-profile')

        with open("./dbt-profile/profiles.yml", 'w') as f:
            yaml.safe_dump(self.custom_profile_config(), f, default_flow_style=True)

        self.run_sql_file("seed_custom.sql")

    def tearDown(self):
        self.run_sql(f"DROP SCHEMA IF EXISTS {self.custom_schema} CASCADE;")
        super().tearDown()

    def custom_profile_config(self):
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'default': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': self.database_host,
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.custom_schema
                    },
                },
                'target': 'default',
            }
        }

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
        workdir = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            self._run_simple_dbt_commands(workdir)
            os.chdir(workdir)

    @use_profile('postgres')
    def test_postgres_dbt_commands_with_relative_dir_as_project_dir(self):
        workdir = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            self._run_simple_dbt_commands(os.path.relpath(workdir, tmpdir))
            os.chdir(workdir)

    def _run_simple_dbt_commands(self, project_dir):
        self.run_dbt(['deps', '--project-dir', project_dir])
        self.run_dbt(['seed', '--project-dir', project_dir])
        self.run_dbt(['run', '--project-dir', project_dir])
        self.run_dbt(['test', '--project-dir', project_dir])
