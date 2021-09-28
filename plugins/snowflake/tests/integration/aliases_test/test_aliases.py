from tests.integration.base import DBTIntegrationTest, use_profile


class TestAliases(DBTIntegrationTest):
    @property
    def schema(self):
        return "aliases"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ['macros'],
            "models": {
                "test": {
                    "alias_in_project": {
                        "alias": 'project_alias',
                    },
                    "alias_in_project_with_override": {
                        "alias": 'project_alias',
                    },
                }
            }
        }

    @use_profile('snowflake')
    def test__alias_model_name_snowflake(self):
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 4)
        self.run_dbt(['test'])
