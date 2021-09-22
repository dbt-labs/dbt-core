from tests.integration.base import DBTIntegrationTest, use_profile


class TestSimpleSeedColumnOverride(DBTIntegrationTest):

    @property
    def schema(self):
        return "simple_seed"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data-config'],
            'macro-paths': ['macros'],
            'seeds': {
                'test': {
                    'enabled': False,
                    'quote_columns': True,
                    'seed_enabled': {
                        'enabled': True,
                        '+column_types': self.seed_enabled_types()
                    },
                    'seed_tricky': {
                        'enabled': True,
                        '+column_types': self.seed_tricky_types(),
                    },
                },
            },
        }

    @property
    def models(self):
        return "models-rs"

    @property
    def profile_config(self):
        return self.redshift_profile()

    def seed_enabled_types(self):
        return {
            "id": "text",
            "birthday": "date",
        }

    def seed_tricky_types(self):
        return {
            'id_str': 'text',
            'looks_like_a_bool': 'text',
            'looks_like_a_date': 'text',
        }

    @use_profile('redshift')
    def test_redshift_simple_seed_with_column_override_redshift(self):
        results = self.run_dbt(["seed", "--show"])
        self.assertEqual(len(results),  2)
        results = self.run_dbt(["test"])
        self.assertEqual(len(results),  10)
