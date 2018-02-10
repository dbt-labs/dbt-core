from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest
import yaml


class TestCLIVars(DBTIntegrationTest):
    @property
    def schema(self):
        return "cli_vars_028"

    @property
    def models(self):
        return "test/integration/028_cli_vars/models_complex"

    @attr(type='postgres')
    def test__cli_vars_longform(self):
        self.use_default_project()
        self.use_profile('postgres')

        cli_vars = {
            "variable_1": "abc",
            "variable_2": ["def", "ghi"],
            "variable_3": {
                "value": "jkl"
            }
        }
        self.run_dbt(["run", "--vars", yaml.dump(cli_vars)])
        self.run_dbt(["test"])


class TestCLIVarsSimple(DBTIntegrationTest):
    @property
    def schema(self):
        return "cli_vars_028"

    @property
    def models(self):
        return "test/integration/028_cli_vars/models_simple"

    @attr(type='postgres')
    def test__cli_vars_shorthand(self):
        self.use_default_project()
        self.use_profile('postgres')

        self.run_dbt(["run", "--vars", "simple: abc"])
        self.run_dbt(["test"])

    @attr(type='postgres')
    def test__cli_vars_longer(self):
        self.use_default_project()
        self.use_profile('postgres')

        self.run_dbt(["run", "--vars", "{simple: abc, unused: def}"])
        self.run_dbt(["test"])
