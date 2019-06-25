from test.integration.base import DBTIntegrationTest, use_profile
import os
import shutil
import yaml


class ModelCopyingIntegrationTest(DBTIntegrationTest):
    def _symlink_test_folders(self):
        # dbt's normal symlink behavior breaks this test, so special-case it
        for entry in os.listdir(self.test_original_source_path):
            src = os.path.join(self.test_original_source_path, entry)
            tst = os.path.join(self.test_root_dir, entry)
            if entry == 'models':
                shutil.copytree(src, tst)
            elif os.path.isdir(entry) or entry.endswith('.sql'):
                os.symlink(src, tst)


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
    def test_toplevel_dbt_run(self):
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 1)
        self.assertTablesEqual("seed", "model")

    @use_profile('postgres')
    def test_subdir_dbt_run(self):
        os.chdir(os.path.join(self.models, "subdir1"))

        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 1)
        self.assertTablesEqual("seed", "model")


class TestCLIInvocationWithProfilesDir(ModelCopyingIntegrationTest):

    def setUp(self):
        super().setUp()

        self.run_sql("DROP SCHEMA IF EXISTS {} CASCADE;".format(self.custom_schema))
        self.run_sql("CREATE SCHEMA {};".format(self.custom_schema))

        # the test framework will remove this in teardown for us.
        if not os.path.exists('./dbt-profile'):
            os.makedirs('./dbt-profile')

        with open("./dbt-profile/profiles.yml", 'w') as f:
            yaml.safe_dump(self.custom_profile_config(), f, default_flow_style=True)

        self.run_sql_file("seed_custom.sql")

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
    def test_toplevel_dbt_run_with_profile_dir_arg(self):
        results = self.run_dbt(['run', '--profiles-dir', 'dbt-profile'], profiles_dir=False)
        self.assertEqual(len(results), 1)

        actual = self.run_sql("select id from {}.model".format(self.custom_schema), fetch='one')

        expected = (1, )
        self.assertEqual(actual, expected)

        res = self.run_dbt(['test', '--profiles-dir', 'dbt-profile'], profiles_dir=False)

        # make sure the test runs against `custom_schema`
        for test_result in res:
            self.assertTrue(self.custom_schema,
                            test_result.node.get('wrapped_sql'))
