from tests.integration.base import DBTIntegrationTest, FakeArgs, use_profile
import os

from dbt.task.test import TestTask
from dbt.exceptions import CompilationException
from dbt.contracts.results import TestStatus

class TestBQSchemaTests(DBTIntegrationTest):
    @property
    def schema(self):
        return "schema_tests"

    @property
    def models(self):
        return "models-v2/bq-models"

    @staticmethod
    def dir(path):
        return os.path.normpath(
            os.path.join('models-v2', path))

    def run_schema_validations(self):
        args = FakeArgs()

        test_task = TestTask(args, self.config)
        return test_task.run()

    @use_profile('bigquery')
    def test_schema_tests_bigquery(self):
        self.use_default_project({'data-paths': [self.dir('seed')]})
        self.assertEqual(len(self.run_dbt(['seed'])), 1)
        results = self.run_dbt()
        self.assertEqual(len(results), 1)
        test_results = self.run_schema_validations()
        self.assertEqual(len(test_results), 8)

        for result in test_results:
            # assert that all deliberately failing tests actually fail
            if 'failure' in result.node.name:
                self.assertEqual(result.status, 'fail')
                self.assertFalse(result.skipped)
                self.assertTrue(
                    result.failures > 0,
                    'test {} did not fail'.format(result.node.name)
                )
            # assert that actual tests pass
            else:
                self.assertEqual(result.status, 'pass')
                self.assertFalse(result.skipped)
                self.assertEqual(
                    result.failures, 0,
                    'test {} failed'.format(result.node.name)
                )

        self.assertEqual(sum(x.failures for x in test_results), 0)
