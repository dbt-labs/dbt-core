from test.integration.base import DBTIntegrationTest, use_profile

class TestGraphSelection(DBTIntegrationTest):

    @property
    def schema(self):
        return "graph_selection_tests_007"

    @property
    def models(self):
        return "test/integration/007_graph_selection_tests/models"

    @property
    def project_config(self):
        return {
            "models": {
                "test": {
                    "users": {
                        "tags": "specified_as_string"
                    },

                    "users_rollup": {
                        "tags": ["specified_in_project"]
                    }
                }
            }
        }

    @use_profile('postgres')
    def test__postgres__select_tag(self):
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        results = self.run_dbt(['run', '--models', 'tag:specified_as_string'])
        self.assertEqual(len(results), 1)

        models_run = [r.node['name'] for r in results]
        self.assertTrue('users' in models_run)


    @use_profile('postgres')
    def test__postgres__select_tag_and_children(self):
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        results = self.run_dbt(['run', '--models', '+tag:specified_in_project+'])
        self.assertEqual(len(results), 2)

        models_run = [r.node['name'] for r in results]
        self.assertTrue('users' in models_run)
        self.assertTrue('users_rollup' in models_run)


    # check that model configs aren't squashed by project configs
    @use_profile('postgres')
    def test__postgres__select_tag_in_model_with_project_Config(self):
        self.run_sql_file("test/integration/007_graph_selection_tests/seed.sql")

        results = self.run_dbt(['run', '--models', 'tag:bi'])
        self.assertEqual(len(results), 2)

        models_run = [r.node['name'] for r in results]
        self.assertTrue('users' in models_run)
        self.assertTrue('users_rollup' in models_run)

