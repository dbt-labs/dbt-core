from test.integration.base import DBTIntegrationTest, FakeArgs, use_profile

from dbt.task.test import TestTask
import os


class TestDataTests(DBTIntegrationTest):

    test_path = os.path.normpath("tests")

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "test-paths": [self.test_path]
        }

    @property
    def schema(self):
        return "data_tests_009"

    @property
    def models(self):
        return "models"

    def run_data_validations(self):
        args = FakeArgs()

        test_task = TestTask(args, self.config)
        return test_task.run()

    @use_profile('postgres')
    def test_postgres_data_tests(self):
        self.use_profile('postgres')

        self.run_sql_file("seed.sql")

        results = self.run_dbt()
        self.assertEqual(len(results), 1)
        test_results = self.run_data_validations()

        for result in test_results:
            # assert that all deliberately failing tests actually fail
            if 'fail' in result.node.name:
                self.assertEqual(result.status, "fail")
                self.assertFalse(result.skipped)
                self.assertTrue(result.failures > 0)
            # assert that actual tests pass
            else:
                self.assertEqual(result.status, 'pass')
                self.assertFalse(result.skipped)
                self.assertEqual(result.failures, 0)

        # check that all tests were run
        defined_tests = os.listdir(self.test_path)
        self.assertNotEqual(len(test_results), 0)
        self.assertEqual(len(test_results), len(defined_tests))
