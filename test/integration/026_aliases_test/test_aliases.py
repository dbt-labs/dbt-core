from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest


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

    @attr(type='postgres')
    def test__alias_model_name(self):
        self.run_dbt(['run'])
        self.run_dbt(['test'])

class TestAliasErrors(DBTIntegrationTest):
    @property
    def schema(self):
        return "aliases_026"

    @property
    def models(self):
        return "test/integration/026_aliases_test/models-dupe"

    @attr(type='postgres')
    def test__alias_dupe_throws_exception(self):
        message = ".*identical database representation.*"
        with self.assertRaisesRegexp(Exception, message):
            self.run_dbt(['run'])

class TestAliaseWithMacroConfig(DBTIntegrationTest):
    @property
    def schema(self):
        return "aliases_026"

    @property
    def models(self):
        return "test/integration/026_aliases_test/models-with-config"

    @property
    def project_config(self):
        return {
            "macro-paths": ['test/integration/026_aliases_test/macros']
        }

    @attr(type='postgres')
    def test__adds_alias_suffix(self):
        self.run_dbt(['run'])
        self.run_dbt(['test'])
