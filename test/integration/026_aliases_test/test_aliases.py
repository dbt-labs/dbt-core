from test.integration.base import DBTIntegrationTest, use_profile


class TestAliases(DBTIntegrationTest):
    @property
    def schema(self):
        return "aliases_026"

    @property
    def models(self):
        return "test/integration/026_aliases_test/models"

    @property
    def project_config(self):
        return {
            "macro-paths": ['test/integration/026_aliases_test/macros'],
            "models": {
                "test": {
                    "alias_in_project": {
                        "alias" : 'project_alias'
                    },
                    "alias_in_project_with_override": {
                        "alias" : 'project_alias'
                    }
                }
            }
        }

    @use_profile('postgres')
    def test__alias_model_name(self):
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 4)
        self.run_dbt(['test'])

    @use_profile('bigquery')
    def test__alias_model_name_bigquery(self):
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 4)
        self.run_dbt(['test'])

    @use_profile('snowflake')
    def test__alias_model_name_snowflake(self):
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 4)
        self.run_dbt(['test'])

class TestAliasErrors(DBTIntegrationTest):
    @property
    def schema(self):
        return "aliases_026"

    @property
    def models(self):
        return "test/integration/026_aliases_test/models-dupe"

    @property
    def project_config(self):
        return {
            "macro-paths": ['test/integration/026_aliases_test/macros'],
        }

    @use_profile('postgres')
    def test__alias_dupe_throws_exception(self):
        message = ".*identical database representation.*"
        with self.assertRaisesRegexp(Exception, message):
            self.run_dbt(['run'])

class TestSameAliasDifferentSchemas(DBTIntegrationTest):
    @property
    def schema(self):
        return "aliases_026"

    @property
    def models(self):
        return "test/integration/026_aliases_test/models-dupe-custom-schema"

    @property
    def project_config(self):
        return {
            "macro-paths": ['test/integration/026_aliases_test/macros'],
        }

    @use_profile('postgres')
    def test__same_alias_succeeds_in_different_schemas(self):
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 3)
        res = self.run_dbt(['test'])

        # Make extra sure the tests ran
        self.assertTrue(len(res) > 0)
