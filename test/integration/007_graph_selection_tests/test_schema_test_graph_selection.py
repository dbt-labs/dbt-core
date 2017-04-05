from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest, FakeArgs

from dbt.task.test import TestTask
from dbt.project import read_project


class TestSchemaTestGraphSelection(DBTIntegrationTest):

    @property
    def schema(self):
        return "graph_selection_tests_007"

    @property
    def models(self):
        return "test/integration/007_graph_selection_tests/models"

    @property
    def project_config(self):
        return {
            "repositories": [
                'https://github.com/fishtown-analytics/dbt-integration-project'
            ]
        }

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.use_default_project()
        self.project = read_project('dbt_project.yml')
        self.expected_ephemeral = []

    def run_schema_and_assert(self, include, exclude, expected_tests):
        self.use_profile('postgres')

        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")
        self.run_dbt(["deps"])
        self.run_dbt()

        args = FakeArgs()
        args.models = include
        args.exclude = exclude

        test_task = TestTask(args, self.project)
        test_results = test_task.run()

        print(test_results)

        ran_tests = sorted([test.node.get('name') for test in test_results])
        expected_sorted = sorted(expected_tests + self.expected_ephemeral)

        self.assertEqual(ran_tests, expected_sorted)

    @attr(type='postgres')
    def test__postgres__schema_tests_no_specifiers(self):
        self.run_schema_and_assert(
            None,
            None,
            ['base_users',
             'unique_table_id',
             'unique_users_id',
             'unique_users_rollup_gender']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_specify_model(self):
        self.run_schema_and_assert(
            ['users'],
            None,
            ['base_users', 'unique_users_id']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_specify_model_and_children(self):
        self.run_schema_and_assert(
            ['users+'],
            None,
            ['base_users', 'unique_users_id', 'unique_users_rollup_gender']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_specify_model_and_parents(self):
        self.run_schema_and_assert(
            ['+users_rollup'],
            None,
            ['base_users', 'unique_users_id', 'unique_users_rollup_gender']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_specify_model_and_parents_with_exclude(self):
        self.run_schema_and_assert(
            ['+users_rollup'],
            ['users_rollup'],
            ['base_users', 'unique_users_id']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_specify_exclude_only(self):
        self.run_schema_and_assert(
            None,
            ['users_rollup'],
            ['base_users', 'unique_table_id', 'unique_users_id']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_specify_model_in_pkg(self):
        self.run_schema_and_assert(
            ['test.users_rollup'],
            None,
            # TODO: change this. there's no way to select only direct ancestors
            # atm.
            ['base_users', 'unique_users_rollup_gender']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_with_glob(self):
        self.run_schema_and_assert(
            ['*'],
            ['users'],
            ['base_users', 'unique_table_id', 'unique_users_rollup_gender']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_dep_package_only(self):
        self.run_schema_and_assert(
            ['dbt_integration_project'],
            None,
            ['unique_table_id']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_model_in_dep_pkg(self):
        self.run_schema_and_assert(
            ['dbt_integration_project.table'],
            None,
            ['unique_table_id']
        )

    @attr(type='postgres')
    def test__postgres__schema_tests_exclude_pkg(self):
        self.run_schema_and_assert(
            None,
            ['dbt_integration_project'],
            ['base_users', 'unique_users_id', 'unique_users_rollup_gender']
        )
