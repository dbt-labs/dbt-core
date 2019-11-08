from test.integration.base import DBTIntegrationTest, use_profile
import json

class TestBaseBigQueryResults(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test_022"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'macro-paths': ['macros'],
        }

    @use_profile('bigquery')
    def test__bigquery_type_inference(self):
        _, test_results = self.run_dbt(['run-operation', 'test_int_inference'])
        self.assertEqual(len(test_results), 1)

        actual_0 = test_results.rows[0]['int_0']
        actual_1 = test_results.rows[0]['int_1']
        actual_2 = test_results.rows[0]['int_2']

        self.assertEqual(actual_0, 0)
        self.assertEqual(str(actual_0), '0')
        self.assertEqual(actual_0 * 2, 0) #  not 00

        self.assertEqual(actual_1, 1)
        self.assertEqual(str(actual_1), '1')
        self.assertEqual(actual_1 * 2, 2) # not 11

        self.assertEqual(actual_2, 2)
        self.assertEqual(str(actual_2), '2')
        self.assertEqual(actual_2 * 2, 4) # not 22
