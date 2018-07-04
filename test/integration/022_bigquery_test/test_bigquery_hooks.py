from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest, FakeArgs


class TestBigQueryHooks(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test_022"

    @property
    def models(self):
        return "test/integration/022_bigquery_test/hook-models"

    @property
    def project_config(self):
        return {
            'data-paths': ['test/integration/022_bigquery_test/data'],
            'test-paths': ['test/integration/022_bigquery_test/hook-tests'],
        }

    @property
    def profile_config(self):
        return self.bigquery_profile()

    @attr(type='bigquery')
    def test__bigquery_hooks(self):
        self.use_profile('bigquery')
        self.use_default_project()
        # make sure seed works twice. Full-refresh is a no-op
        self.run_dbt(['seed'])
        self.run_dbt(['seed', '--full-refresh'])

        # Run models, and check that hooks inserted new into the base table.
        self.run_dbt()
        self.run_dbt(['test'], expect_pass=True)
