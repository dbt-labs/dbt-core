from tests.integration.base import DBTIntegrationTest, use_profile


class TestBaseCaching(DBTIntegrationTest):
    @property
    def schema(self):
        return "caching"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'test-paths': ['tests']
        }

    @use_profile('redshift')
    def test_redshift_adapter_methods(self):
        self.run_dbt(['compile'])  # trigger any compile-time issues
        self.run_dbt()
        self.assertTablesEqual('model', 'expected')
