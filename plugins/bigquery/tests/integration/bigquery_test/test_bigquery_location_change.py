from tests.integration.base import DBTIntegrationTest, use_profile
import os


class TestBigqueryErrorHandling(DBTIntegrationTest):
    def setUp(self):
        self.valid_location = os.getenv('DBT_TEST_BIGQUERY_INITIAL_LOCATION', 'US')
        self.invalid_location = os.getenv('DBT_TEST_BIGQUERY_BAD_LOCATION', 'northamerica-northeast1')
        self.location = self.valid_location
        super().setUp()

    @property
    def schema(self):
        return "bigquery_test"

    @property
    def models(self):
        return "location-models"

    def bigquery_profile(self):
        result = super().bigquery_profile()
        result['test']['outputs']['default2']['location'] = self.location
        return result

    @use_profile('bigquery')
    def test_bigquery_location_invalid(self):
        self.run_dbt()
        self.location = self.invalid_location
        self.use_profile('bigquery')
        _, stdout = self.run_dbt_and_capture(expect_pass=False)
        assert 'Query Job SQL Follows' not in stdout
