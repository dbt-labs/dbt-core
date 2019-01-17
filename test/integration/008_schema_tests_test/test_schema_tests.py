from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest, FakeArgs

from dbt.task.test import TestTask


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
        return "test/integration/008_schema_tests_test/models-v1/models"

    def run_schema_validations(self):
        args = FakeArgs()

        test_task = TestTask(args, self.config)
        return test_task.run()

    @attr(type='postgres')
    def test_schema_tests(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 5)
        test_results = self.run_schema_validations()
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
        return "test/integration/008_schema_tests_test/models-v1/malformed"

    def run_schema_validations(self):
        args = FakeArgs()

        test_task = TestTask(args, self.config)
        return test_task.run()

    @attr(type='postgres')
    def test_malformed_schema_test_wont_brick_run(self):
        # dbt run should work (Despite broken schema test)
        results = self.run_dbt()
        self.assertEqual(len(results), 1)

        ran_tests = self.run_schema_validations()
        self.assertEqual(len(ran_tests), 2)
        self.assertEqual(sum(x.status for x in ran_tests), 0)


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
            "packages": [
                {
                    'git': 'https://github.com/fishtown-analytics/dbt-utils',
                    'revision': '0.13-support',
                },
                {'git': 'https://github.com/fishtown-analytics/dbt-integration-project'},
            ]
        }


    @property
    def project_config(self):
        # dbt-utils containts a schema test (equality)
        # dbt-integration-project contains a schema.yml file
        # both should work!
        return {
            "macro-paths": ["test/integration/008_schema_tests_test/macros-v1"],
        }

    @property
    def models(self):
        return "test/integration/008_schema_tests_test/models-v1/custom"

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
