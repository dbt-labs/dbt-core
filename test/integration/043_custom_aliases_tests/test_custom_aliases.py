from test.integration.base import DBTIntegrationTest, use_profile


class TestAliases(DBTIntegrationTest):
    @property
    def schema(self):
        return "custom_aliases_043"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ['macros'],
        }

    @use_profile('postgres')
    def test_postgres_customer_alias_name(self):
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 2)
        self.run_dbt(['test'])


class TestAliasesWithConfig(TestAliases):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ['macros-configs'],
        }

    @use_profile('postgres')
    def test_postgres_customer_alias_name(self):
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 2)
        self.run_dbt(['test'])
