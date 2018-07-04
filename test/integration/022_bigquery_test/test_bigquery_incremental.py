from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest, FakeArgs


class TestBigQueryIncremental(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test_022"

    @property
    def models(self):
        return "test/integration/022_bigquery_test/incr-models"

    @property
    def project_config(self):
        return {
            'data-paths': ['test/integration/022_bigquery_test/data'],
            'test-paths': ['test/integration/022_bigquery_test/incr-tests'],
        }

    @property
    def profile_config(self):
        return self.bigquery_profile()

    @attr(type='bigquery')
    def test__bigquery_incremental_unique(self):
        self.use_profile('bigquery')
        self.use_default_project()
        # make sure seed works twice. Full-refresh is a no-op
        self.run_dbt(['seed'])
        self.run_dbt(['seed', '--full-refresh'])

        # Create the initial incremental tables.
        self.run_dbt()
        self.run_dbt(['test'], expect_pass=True)

        # If we run again, the tables should still be equal.
        self.run_dbt()
        self.run_dbt(['test'], expect_pass=True)
