from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest, FakeArgs

from dbt.task.test import TestTask
from dbt.project import read_project


class TestSchemaTests(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("test/integration/008_schema_tests_test/seed.sql")
        self.run_sql_file("test/integration/008_schema_tests_test/seed_failure.sql")

    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "test/integration/008_schema_tests_test/models"

    def run_schema_validations(self):
        project = read_project('dbt_project.yml')
        args = FakeArgs()

        test_task = TestTask(args, project)
        print(project)
        return test_task.run()

    @attr(type='postgres')
    def test_schema_tests(self):
        self.run_dbt()
        test_results = self.run_schema_validations()

        for result in test_results:
            # assert that all deliberately failing tests actually fail
            if 'failure' in result.node.get('name'):
                self.assertFalse(result.errored)
                self.assertFalse(result.skipped)
                self.assertTrue(result.status > 0)

            # assert that actual tests pass
            else:
                self.assertFalse(result.errored)
                self.assertFalse(result.skipped)
                # status = # of failing rows
                self.assertEqual(result.status, 0)


class TestMalformedSchemaTests(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("test/integration/008_schema_tests_test/seed.sql")

    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "test/integration/008_schema_tests_test/models-malformed"

    def run_schema_validations(self):
        project = read_project('dbt_project.yml')
        args = FakeArgs()

        test_task = TestTask(args, project)
        return test_task.run()

    @attr(type='postgres')
    def test_malformed_schema_test_wont_brick_run(self):
        # dbt run should work (Despite broken schema test)
        self.run_dbt()

        ran_tests = self.run_schema_validations()
        self.assertEqual(len(ran_tests), 2)


class TestCustomSchemaTests(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("test/integration/008_schema_tests_test/seed.sql")

    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def project_config(self):
        return {
            "macro-paths": ["test/integration/008_schema_tests_test/macros"],
        }

    @property
    def models(self):
        return "test/integration/008_schema_tests_test/models-custom"

    def run_schema_validations(self):
        project = read_project('dbt_project.yml')
        args = FakeArgs()

        test_task = TestTask(args, project)
        return test_task.run()

    @attr(type='postgres')
    def test_schema_tests(self):
        self.run_dbt()
        test_results = self.run_schema_validations()

        for result in test_results:
            print(result.node, result.errored, result.skipped, result.status)
