from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest

class TestGraphSelection(DBTIntegrationTest):

    @property
    def schema(self):
        return "graph_selection_tests_007"

    @property
    def models(self):
        return "test/integration/007_graph_selection_tests/models"

    def assert_correct_schemas(self):
        exists = self.adapter.check_schema_exists(
            self.default_database,
            self.unique_schema(),
            '__test'
        )
        self.assertTrue(exists)

        schema = self.unique_schema()+'_and_then'
        exists = self.adapter.check_schema_exists(
            self.default_database,
            schema,
            '__test'
        )
        self.assertFalse(exists)

    @attr(type='postgres')
    def test__postgres__specific_model(self):
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        results = self.run_dbt(['run', '--models', 'users'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)
        self.assert_correct_schemas()

    @attr(type='postgres')
    def test__postgres__tags(self):
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        results = self.run_dbt(['run', '--models', 'tag:bi'])
        self.assertEqual(len(results), 2)

        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)
        self.assertTrue('users' in created_models)
        self.assertTrue('users_rollup' in created_models)
        self.assert_correct_schemas()

    @attr(type='postgres')
    def test__postgres__tags_and_children(self):
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        results = self.run_dbt(['run', '--models', 'tag:base+'])
        self.assertEqual(len(results), 3)

        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)
        self.assertIn('emails_alt', created_models)
        self.assertTrue('users_rollup' in created_models)
        self.assertTrue('users' in created_models)
        self.assert_correct_schemas()

    @attr(type='snowflake')
    def test__snowflake__specific_model(self):
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        results = self.run_dbt(['run', '--models', 'users'])
        self.assertEqual(len(results),  1)

        self.assertTablesEqual("SEED", "USERS")
        created_models = self.get_models_in_schema()
        self.assertFalse('USERS_ROLLUP' in created_models)
        self.assertFalse('BASE_USERS' in created_models)
        self.assertFalse('EMAILS' in created_models)
        self.assert_correct_schemas()

    @attr(type='postgres')
    def test__postgres__specific_model_and_children(self):
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        results = self.run_dbt(['run', '--models', 'users+'])
        self.assertEqual(len(results),  3)

        self.assertTablesEqual("seed", "users")
        self.assertTablesEqual("summary_expected", "users_rollup")
        created_models = self.get_models_in_schema()
        self.assertIn('emails_alt', created_models)
        self.assertNotIn('base_users', created_models)
        self.assertNotIn('emails', created_models)
        self.assert_correct_schemas()

    @attr(type='snowflake')
    def test__snowflake__specific_model_and_children(self):
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        results = self.run_dbt(['run', '--models', 'users+'])
        self.assertEqual(len(results),  2)

        self.assertManyTablesEqual(
            ["SEED", "USERS"],
            ["SUMMARY_EXPECTED", "USERS_ROLLUP"]
        )
        created_models = self.get_models_in_schema()
        self.assertFalse('BASE_USERS' in created_models)
        self.assertFalse('EMAILS' in created_models)


    @attr(type='postgres')
    def test__postgres__specific_model_and_parents(self):
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        results = self.run_dbt(['run', '--models', '+users_rollup'])
        self.assertEqual(len(results),  2)

        self.assertTablesEqual("seed", "users")
        self.assertTablesEqual("summary_expected", "users_rollup")
        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)
        self.assert_correct_schemas()

    @attr(type='snowflake')
    def test__snowflake__specific_model_and_parents(self):
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        results = self.run_dbt(['run', '--models', '+users_rollup'])
        self.assertEqual(len(results),  2)

        self.assertManyTablesEqual(
            ["SEED", "USERS"],
            ["SUMMARY_EXPECTED", "USERS_ROLLUP"]
        )

        created_models = self.get_models_in_schema()
        self.assertFalse('BASE_USERS' in created_models)
        self.assertFalse('EMAILS' in created_models)


    @attr(type='postgres')
    def test__postgres__specific_model_with_exclusion(self):
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        results = self.run_dbt(
            ['run', '--models', '+users_rollup', '--exclude', 'users_rollup']
        )
        self.assertEqual(len(results),  1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('emails' in created_models)
        self.assert_correct_schemas()

    @attr(type='snowflake')
    def test__snowflake__specific_model_with_exclusion(self):
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        results = self.run_dbt(
            ['run', '--models', '+users_rollup', '--exclude', 'users_rollup']
        )
        self.assertEqual(len(results),  1)

        self.assertManyTablesEqual(["SEED", "USERS"])
        created_models = self.get_models_in_schema()
        self.assertFalse('BASE_USERS' in created_models)
        self.assertFalse('USERS_ROLLUP' in created_models)
        self.assertFalse('EMAILS' in created_models)

    @attr(type='postgres')
    def test__postgres__locally_qualified_name(self):
        results = self.run_dbt(['run', '--models', 'test.subdir'])
        self.assertEqual(len(results), 2)

        created_models = self.get_models_in_schema()
        self.assertNotIn('users_rollup', created_models)
        self.assertNotIn('base_users', created_models)
        self.assertNotIn('emails', created_models)
        self.assertIn('subdir', created_models)
        self.assertIn('nested_users', created_models)
        self.assert_correct_schemas()

    @attr(type='postgres')
    def test__postgres__childrens_parents(self):
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")
        results = self.run_dbt(['run', '--models', '@base_users'])
        self.assertEqual(len(results), 3)

        created_models = self.get_models_in_schema()
        self.assertIn('users_rollup', created_models)
        self.assertIn('users', created_models)
        self.assertIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)

    @attr(type='postgres')
    def test__postgres__more_childrens_parents(self):
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")
        results = self.run_dbt(['run', '--models', '@users'])
        # base_users, emails, users_rollup, but not users (ephemeral)
        self.assertEqual(len(results), 3)

        created_models = self.get_models_in_schema()
        self.assertIn('users_rollup', created_models)
        self.assertIn('users', created_models)
        self.assertIn('emails_alt', created_models)
        self.assertNotIn('subdir', created_models)
        self.assertNotIn('nested_users', created_models)
