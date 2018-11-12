
from test.integration.base import DBTIntegrationTest, use_profile


class TestThreadCount(DBTIntegrationTest):

    @property
    def project_config(self):
        return {}

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
        return "test/integration/031_thread_count_test/models"

    @use_profile('postgres')
    def test_postgres_threading_8x(self):
        results = self.run_dbt(args=['run', '--threads', '16'])
        self.assertTrue(len(results), 20)
