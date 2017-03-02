from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest

class TestGraphSelection(DBTIntegrationTest):

    @property
    def schema(self):
        return "graph_selection_tests_007"

    @property
    def models(self):
        return "test/integration/007_graph_selection_tests/models"

    @attr(type='postgres')
    def test__postgres__specific_model(self):
        self.use_default_project()
        self.use_profile('postgres')
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        self.run_dbt(['run', '--models', 'users'])

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)

    @attr(type='snowflake')
    def test__snowflake__specific_model(self):
        self.use_default_project()
        self.use_profile('snowflake')
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        self.run_dbt(['run', '--models', 'users'])

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)


    @attr(type='postgres')
    def test__postgres__specific_model_and_children(self):
        self.use_default_project()
        self.use_profile('postgres')
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        self.run_dbt(['run', '--models', 'users+'])

        self.assertTablesEqual("seed", "users")
        self.assertTablesEqual("summary_expected", "users_rollup")
        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)

    @attr(type='snowflake')
    def test__snowflake__specific_model_and_children(self):
        self.use_default_project()
        self.use_profile('snowflake')
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        self.run_dbt(['run', '--models', 'users+'])

        self.assertTablesEqual("seed", "users")
        self.assertTablesEqual("summary_expected", "users_rollup")
        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)


    @attr(type='postgres')
    def test__postgres__specific_model_and_parents(self):
        self.use_default_project()
        self.use_profile('postgres')
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        self.run_dbt(['run', '--models', '+users_rollup'])

        self.assertTablesEqual("seed", "users")
        self.assertTablesEqual("summary_expected", "users_rollup")
        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)

    @attr(type='snowflake')
    def test__snowflake__specific_model_and_parents(self):
        self.use_default_project()
        self.use_profile('snowflake')
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        self.run_dbt(['run', '--models', '+users_rollup'])

        self.assertTablesEqual("seed", "users")
        self.assertTablesEqual("summary_expected", "users_rollup")
        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)


    @attr(type='postgres')
    def test__postgres__specific_model_with_exclusion(self):
        self.use_default_project()
        self.use_profile('postgres')
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        self.run_dbt(['run', '--models', '+users_rollup', '--exclude', 'users_rollup'])

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)
        self.assertFalse('users_rollup' in created_models)

    @attr(type='snowflake')
    def test__snowflake__specific_model_with_exclusion(self):
        self.use_default_project()
        self.use_profile('snowflake')
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        self.run_dbt(['run', '--models', '+users_rollup', '--exclude', 'users_rollup'])

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('base_users' in created_models)
        self.assertFalse('users_rollup' in created_models)
