
from test.integration.base import DBTIntegrationTest, use_profile


class TestThreadCount(DBTIntegrationTest):

    @property
    def project_config(self):
        return {'config-version': 2}

    @property
    def profile_config(self):
        return {
            'threads': 2,
        }

    @property
    def schema(self):
        return "thread_tests_031"

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test_postgres_threading_8x(self):
        results = self.run_dbt(args=['run', '--threads', '16'])
        self.assertTrue(len(results), 20)
