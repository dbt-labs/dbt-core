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


class TestSimpleSeedColumnOverrideBQ(TestSimpleSeedColumnOverride):
    @property
    def models(self):
        return "models-bq"

    def seed_enabled_types(self):
        return {
            "id": "FLOAT64",
            "birthday": "STRING",
        }

    def seed_tricky_types(self):
        return {
            'id_str': 'STRING',
            'looks_like_a_bool': 'STRING',
            'looks_like_a_date': 'STRING',
        }

    @property
    def profile_config(self):
        return self.bigquery_profile()

    @use_profile('bigquery')
    def test_bigquery_simple_seed_with_column_override_bigquery(self):
        results = self.run_dbt(["seed", "--show"])
        self.assertEqual(len(results),  2)
        results = self.run_dbt(["test"])
        self.assertEqual(len(results),  10)
