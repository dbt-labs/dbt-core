from tests.integration.base import DBTIntegrationTest, use_profile


class TestBaseBigQueryResults(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['macros'],
        }

    @use_profile('bigquery')
    def test__bigquery_type_inference(self):
        result = self.run_dbt(['run-operation', 'test_int_inference'])
        self.assertTrue(result.success)
