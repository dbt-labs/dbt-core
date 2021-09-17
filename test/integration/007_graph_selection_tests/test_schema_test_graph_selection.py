from test.integration.base import DBTIntegrationTest, FakeArgs, use_profile

from dbt.task.test import TestTask


class TestSchemaTestGraphSelection(DBTIntegrationTest):

    @property
    def schema(self):
        return "graph_selection_tests_007"

    @property
    def models(self):
        return "models"

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'git': 'https://github.com/dbt-labs/dbt-integration-project',
                    'revision': 'dbt/0.17.0',
                }
            ]
        }

    def run_schema_and_assert(self, include, exclude, expected_tests):
        self.run_sql_file("seed.sql")
        self.run_dbt(["deps"])
        results = self.run_dbt(['run', '--exclude', 'never_selected'])
        self.assertEqual(len(results), 10)

        args = FakeArgs()
        args.select = include
        args.exclude = exclude

        test_task = TestTask(args, self.config)
        test_results = test_task.run()

        ran_tests = sorted([test.node.name for test in test_results])
        expected_sorted = sorted(expected_tests)

        self.assertEqual(ran_tests, expected_sorted)

    @use_profile('postgres')
    def test__postgres__schema_tests_no_specifiers(self):
        self.run_schema_and_assert(
            None,
            None,
            ['not_null_emails_email',
             'unique_table_model_id',
             'unique_users_id',
             'unique_users_rollup_gender']
        )

    @use_profile('postgres')
    def test__postgres__schema_tests_specify_model(self):
        self.run_schema_and_assert(
            ['users'],
            None,
            ['unique_users_id']
        )

    @use_profile('postgres')
    def test__postgres__schema_tests_specify_tag(self):
        self.run_schema_and_assert(
            ['tag:bi'],
            None,
            ['unique_users_id',
             'unique_users_rollup_gender']
        )

    @use_profile('postgres')
    def test__postgres__schema_tests_specify_model_and_children(self):
        self.run_schema_and_assert(
            ['users+'],
            None,
            ['unique_users_id', 'unique_users_rollup_gender']
        )

    @use_profile('postgres')
    def test__postgres__schema_tests_specify_tag_and_children(self):
        self.run_schema_and_assert(
            ['tag:base+'],
            None,
            ['not_null_emails_email',
             'unique_users_id',
             'unique_users_rollup_gender']
        )

    @use_profile('postgres')
    def test__postgres__schema_tests_specify_model_and_parents(self):
        self.run_schema_and_assert(
            ['+users_rollup'],
            None,
            ['unique_users_id', 'unique_users_rollup_gender']
        )

    @use_profile('postgres')
    def test__postgres__schema_tests_specify_model_and_parents_with_exclude(self):
        self.run_schema_and_assert(
            ['+users_rollup'],
            ['users_rollup'],
            ['unique_users_id']
        )

    @use_profile('postgres')
    def test__postgres__schema_tests_specify_exclude_only(self):
        self.run_schema_and_assert(
            None,
            ['users_rollup'],
            ['not_null_emails_email', 'unique_table_model_id', 'unique_users_id']
        )

    @use_profile('postgres')
    def test__postgres__schema_tests_specify_model_in_pkg(self):
        self.run_schema_and_assert(
            ['test.users_rollup'],
            None,
            # TODO: change this. there's no way to select only direct ancestors
            # atm.
            ['unique_users_rollup_gender']
        )

    @use_profile('postgres')
    def test__postgres__schema_tests_with_glob(self):
        self.run_schema_and_assert(
            ['*'],
            ['users'],
            ['not_null_emails_email', 'unique_table_model_id', 'unique_users_rollup_gender']
        )

    @use_profile('postgres')
    def test__postgres__schema_tests_dep_package_only(self):
        self.run_schema_and_assert(
            ['dbt_integration_project'],
            None,
            ['unique_table_model_id']
        )

    @use_profile('postgres')
    def test__postgres__schema_tests_model_in_dep_pkg(self):
        self.run_schema_and_assert(
            ['dbt_integration_project.table_model'],
            None,
            ['unique_table_model_id']
        )

    @use_profile('postgres')
    def test__postgres__schema_tests_exclude_pkg(self):
        self.run_schema_and_assert(
            None,
            ['dbt_integration_project'],
            ['not_null_emails_email', 'unique_users_id', 'unique_users_rollup_gender']
        )
