from test.integration.base import DBTIntegrationTest


class TestAliases(DBTIntegrationTest):
    @property
    def schema(self):
        return "custom_aliases_043"

    @property
    def models(self):
        return "test/integration/043_custom_aliases_test/models"

    @property
    def project_config(self):
        return {
            "macro-paths": ['test/integration/043_custom_aliases_test/macros'],
        }

    def test__customer_alias_name(self):
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 1)
        self.run_dbt(['test'])
