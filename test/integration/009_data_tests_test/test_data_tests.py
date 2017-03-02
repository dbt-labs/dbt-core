from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest, FakeArgs

from dbt.task.test import TestTask
from dbt.project import read_project

class TestDataTests(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("test/integration/009_data_tests_test/seed.sql")

    @property
    def project_config(self):
        return {
            "test-paths": ["test/integration/009_data_tests_test/tests"]
        }

    @property
    def schema(self):
        return "data_tests_009"

    @property
    def models(self):
        return "test/integration/009_data_tests_test/models"

    def run_data_validations(self):
        project = read_project('dbt_project.yml')
        args = FakeArgs()

        test_task = TestTask(args, project)
        return test_task.run()

    @attr(type='postgres')
    def test_data_tests(self):
        self.run_dbt()
        test_results = self.run_data_validations()

        for result in test_results:
            # assert that all deliberately failing tests actually fail
            if 'fail' in result.model.name:
                self.assertFalse(result.errored)
                self.assertFalse(result.skipped)
                self.assertTrue(result.status > 0)

            # assert that actual tests pass
            else:
                self.assertFalse(result.errored)
                self.assertFalse(result.skipped)
                # status = # of failing rows
                self.assertEqual(result.status, 0)
