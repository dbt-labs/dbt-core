from test.integration.base import DBTIntegrationTest, use_profile


class TestContextVars(DBTIntegrationTest):
    @property
    def schema(self):
        return "context_members_013"

    @property
    def models(self):
        return "context-member-models"

    @property
    def project_config(self):
        return {'test-paths': ['tests']}

    @use_profile('postgres')
    def test_json_data_tests_postgres(self):
        self.assertEqual(len(self.run_dbt(['test'])), 2)
