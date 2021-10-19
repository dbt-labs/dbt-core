from test.integration.base import DBTIntegrationTest, use_profile


class TestSimpleSeedColumnOverride(DBTIntegrationTest):

    @property
    def schema(self):
        return "simple_seed_005"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds-config'],
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


class TestSimpleSeedColumnOverridePostgres(TestSimpleSeedColumnOverride):
    @property
    def models(self):
        return "models-pg"

    @property
    def profile_config(self):
        return self.postgres_profile()

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

    @use_profile('postgres')
    def test_postgres_simple_seed_with_column_override_postgres(self):
        results = self.run_dbt(["seed", "--show"])
        self.assertEqual(len(results),  2)
        results = self.run_dbt(["test"])
        self.assertEqual(len(results),  10)

