from test.integration.base import DBTIntegrationTest, use_profile


class TestGraphSelection(DBTIntegrationTest):

    @property
    def schema(self):
        return "graph_selection_tests_007"

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test__postgres__specific_model(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'users'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)
