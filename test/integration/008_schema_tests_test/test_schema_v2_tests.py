from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest, FakeArgs, use_profile
import os

from dbt.task.test import TestTask
from dbt.exceptions import CompilationException


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
        return "test/integration/008_schema_tests_test/models-v2/models"

    def run_schema_validations(self):
        args = FakeArgs()

        test_task = TestTask(args, self.config)
        return test_task.run()

    @attr(type='postgres')
    def test_schema_tests(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 5)
        test_results = self.run_schema_validations()
        # If the disabled model's tests ran, there would be 19 of these.
        self.assertEqual(len(test_results), 18)

        for result in test_results:
            # assert that all deliberately failing tests actually fail
            if 'failure' in result.node.get('name'):
                self.assertFalse(result.errored)
                self.assertFalse(result.skipped)
                self.assertTrue(
                    result.status > 0,
                    'test {} did not fail'.format(result.node.get('name'))
                )

            # assert that actual tests pass
            else:
                self.assertFalse(result.errored)
                self.assertFalse(result.skipped)
                # status = # of failing rows
                self.assertEqual(
                    result.status, 0,
                    'test {} failed'.format(result.node.get('name'))
                )

        self.assertEqual(sum(x.status for x in test_results), 6)

class TestMalformedSchemaTests(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("test/integration/008_schema_tests_test/seed.sql")

    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "test/integration/008_schema_tests_test/models-v2/malformed"

    def run_schema_validations(self):
        args = FakeArgs()

        test_task = TestTask(args, self.config)
        return test_task.run()

    @attr(type='postgres')
    def test_malformed_schema_test_wont_brick_run(self):
        # dbt run should work (Despite broken schema test)
        results = self.run_dbt(strict=False)
        self.assertEqual(len(results), 2)

        # in v2, we skip the entire model
        ran_tests = self.run_schema_validations()
        self.assertEqual(len(ran_tests), 5)
        self.assertEqual(sum(x.status for x in ran_tests), 0)

    # TODO: re-enable this test when we make --strict actually set strict mode
    # @attr(type='postgres')
    # def test_malformed_schema_strict_will_break_run(self):
    #     with self.assertRaises(CompilationException):
    #         self.run_dbt(strict=True)


class TestCustomSchemaTests(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("test/integration/008_schema_tests_test/seed.sql")

    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def packages_config(self):
        return {
            'packages': [
                {
                    'git': 'https://github.com/fishtown-analytics/dbt-utils',
                    'revision': '0.13-support',
                },
                {
                    'git': 'https://github.com/fishtown-analytics/dbt-integration-project',
                },
            ]
        }

    @property
    def project_config(self):
        # dbt-utils containts a schema test (equality)
        # dbt-integration-project contains a schema.yml file
        # both should work!
        return {
            "macro-paths": ["test/integration/008_schema_tests_test/macros-v2"],
        }

    @property
    def models(self):
        return "test/integration/008_schema_tests_test/models-v2/custom"

    def run_schema_validations(self):
        args = FakeArgs()

        test_task = TestTask(args, self.config)
        return test_task.run()

    @attr(type='postgres')
    def test_schema_tests(self):
        self.run_dbt(["deps"])
        results = self.run_dbt()
        self.assertEqual(len(results), 4)

        test_results = self.run_schema_validations()
        self.assertEqual(len(test_results), 6)

        expected_failures = ['unique', 'every_value_is_blue']

        for result in test_results:
            if result.errored:
                self.assertTrue(result.node['name'] in expected_failures)
        self.assertEqual(sum(x.status for x in test_results), 52)


class TestBQSchemaTests(DBTIntegrationTest):
    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "test/integration/008_schema_tests_test/models-v2/bq-models"

    @staticmethod
    def dir(path):
        return os.path.normpath(
            os.path.join('test/integration/008_schema_tests_test/models-v2', path))

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
            if 'failure' in result.node.get('name'):
                self.assertFalse(result.errored)
                self.assertFalse(result.skipped)
                self.assertTrue(
                    result.status > 0,
                    'test {} did not fail'.format(result.node.get('name'))
                )

            # assert that actual tests pass
            else:
                self.assertFalse(result.errored)
                self.assertFalse(result.skipped)
                # status = # of failing rows
                self.assertEqual(
                    result.status, 0,
                    'test {} failed'.format(result.node.get('name'))
                )

        self.assertEqual(sum(x.status for x in test_results), 0)
