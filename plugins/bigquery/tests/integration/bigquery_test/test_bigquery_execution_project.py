import os
from tests.integration.base import DBTIntegrationTest, use_profile


class TestAlternateExecutionProjectBigQueryRun(DBTIntegrationTest):
    @property
    def schema(self):
        return "bigquery_test"

    @property
    def models(self):
        return "execution-project-models"

    @use_profile('bigquery')
    def test__bigquery_execute_project(self):
        results = self.run_dbt(['run', '--models', 'model'])
        self.assertEqual(len(results), 1)
        execution_project = os.environ['BIGQUERY_TEST_ALT_DATABASE']
        self.run_dbt(['test',
                      '--target', 'alternate',
                      '--vars', '{ project_id: %s, unique_schema_id: %s }'
                      % (execution_project, self.unique_schema())],
                     expect_pass=False)
