from test.integration.base import DBTIntegrationTest, use_profile


class TestRefOverride(DBTIntegrationTest):
    @property
    def schema(self):
        return "dbt_ref_override_055"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds'],
            "macro-paths": ["macros"],
            'seeds': {
                'quote_columns': False,
            },
        }

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test_postgres_ref_override(self):
        self.run_dbt(['seed'])
        self.run_dbt(['run'])
        # We want it to equal seed_2 and not seed_1. If it's
        # still pointing at seed_1 then the override hasn't worked.
        self.assertTablesEqual('ref_override', 'seed_2')
