from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest, FakeArgs


class TestSimpleBigQueryRun(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test_022"

    @property
    def models(self):
        return "test/integration/022_bigquery_test/models"

    @property
    def project_config(self):
        return {
            'macro-paths': ['test/integration/022_bigquery_test/macros'],
        }

    @property
    def profile_config(self):
        return self.bigquery_profile()

    @attr(type='bigquery')
    def test__bigquery_simple_run(self):
        self.use_profile('bigquery')
        self.use_default_project()
        self.run_dbt()

        # The 'dupe' model should fail, but all others should pass
        test_results = self.run_dbt(['test'], expect_pass=False)

        for result in test_results:
            if 'dupe' in result.node.get('name'):
                self.assertFalse(result.errored)
                self.assertFalse(result.skipped)
                self.assertTrue(result.status > 0)

            # assert that actual tests pass
            else:
                self.assertFalse(result.errored)
                self.assertFalse(result.skipped)
                # status = # of failing rows
                self.assertEqual(result.status, 0)
