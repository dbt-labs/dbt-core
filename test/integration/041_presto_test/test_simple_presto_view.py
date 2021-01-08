from test.integration.base import DBTIntegrationTest, FakeArgs, use_profile
import random
import time


class TestBasePrestoRun(DBTIntegrationTest):

    @property
    def schema(self):
        return "presto_test_41"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'macro-paths': ['macros'],
            'seeds': {
                'quote_columns': False,
            },
        }

    @property
    def profile_config(self):
        return self.presto_profile()

    def assert_nondupes_pass(self):
        # The 'dupe' model should fail, but all others should pass
        test_results = self.run_dbt(['test'], expect_pass=False)

        for result in test_results:
            if 'dupe' in result.node.name:
                self.assertEqual(result.status, 'fail')
                self.assertFalse(result.skipped)
                self.assertTrue(int(result.message) > 0)

            # assert that actual tests pass
            else:
                self.assertEqual(result.status, 'pass')
                self.assertFalse(result.skipped)
                # message = # of failing rows
                self.assertEqual(int(result.message), 0)


class TestSimplePrestoRun(TestBasePrestoRun):
    def setUp(self):
        super().setUp()
        for conn in self.adapter.connections.in_use.values():
            conn.transaction_open

    @use_profile('presto')
    def test__presto_simple_run(self):
        # make sure seed works twice. Full-refresh is a no-op
        self.run_dbt(['seed'])
        self.run_dbt(['seed', '--full-refresh'])
        results = self.run_dbt()
        self.assertEqual(len(results), 2)
        self.assert_nondupes_pass()


class TestUnderscorePrestoRun(TestBasePrestoRun):
    prefix = "_test{}{:04}".format(int(time.time()), random.randint(0, 9999))

    @use_profile('presto')
    def test_presto_run_twice(self):
        self.run_dbt(['seed'])
        results = self.run_dbt()
        self.assertEqual(len(results), 2)
        self.assert_nondupes_pass()
        results = self.run_dbt()
        self.assertEqual(len(results), 2)
        self.assert_nondupes_pass()
