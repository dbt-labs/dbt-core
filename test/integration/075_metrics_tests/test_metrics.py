from test.integration.base import DBTIntegrationTest, use_profile, normalize, get_manifest
from dbt.exceptions import ParsingException

class BaseMetricTest(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_075"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds'],
            'seeds': {
                'quote_columns': False,
            },
        }

    @use_profile('postgres')
    def test_postgres_simple_metric(self):
        # initial run
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 1)
        manifest = get_manifest()
        metric_ids = list(manifest.metrics.keys())
        expected_metric_ids = ['metric.test.number_of_people', 'metric.test.collective_tenure']
        self.assertEqual(metric_ids, expected_metric_ids)

class InvalidRefMetricTest(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_075"

    @property
    def models(self):
        return "invalid-models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds'],
            'seeds': {
                'quote_columns': False,
            },
        }

    @use_profile('postgres')
    def test_postgres_simple_metric(self):
        # initial run
        with self.assertRaises(ParsingException):
            results = self.run_dbt(["run"])

