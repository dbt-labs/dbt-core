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


class TestSimpleSeedColumnOverrideSnowflake(TestSimpleSeedColumnOverride):
    @property
    def models(self):
        return "models-snowflake"

    def seed_enabled_types(self):
        return {
            "id": "FLOAT",
            "birthday": "TEXT",
        }

    def seed_tricky_types(self):
        return {
            'id_str': 'TEXT',
            'looks_like_a_bool': 'TEXT',
            'looks_like_a_date': 'TEXT',
        }

    @property
    def profile_config(self):
        return self.snowflake_profile()

    @use_profile('snowflake')
    def test_snowflake_simple_seed_with_column_override_snowflake(self):
        results = self.run_dbt(["seed", "--show"])
        self.assertEqual(len(results),  2)
        results = self.run_dbt(["test"])
        self.assertEqual(len(results),  10)
