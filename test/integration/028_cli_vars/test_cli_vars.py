from test.integration.base import DBTIntegrationTest, use_profile
import yaml


class TestCLIVars(DBTIntegrationTest):
    @property
    def schema(self):
        return "cli_vars_028"

    @property
    def models(self):
        return "test/integration/028_cli_vars/models_complex"

    @use_profile('postgres')
    def test__cli_vars_longform(self):
        self.use_profile('postgres')
        self.use_default_project()

        cli_vars = {
            "variable_1": "abc",
            "variable_2": ["def", "ghi"],
            "variable_3": {
                "value": "jkl"
            }
        }
        results = self.run_dbt(["run", "--vars", yaml.dump(cli_vars)])
        self.assertEqual(len(results), 1)
        results = self.run_dbt(["test", "--vars", yaml.dump(cli_vars)])
        self.assertEqual(len(results), 3)


class TestCLIVarsSimple(DBTIntegrationTest):
    @property
    def schema(self):
        return "cli_vars_028"

    @property
    def models(self):
        return "test/integration/028_cli_vars/models_simple"

    @use_profile('postgres')
    def test__cli_vars_shorthand(self):
        self.use_profile('postgres')
        self.use_default_project()

        results = self.run_dbt(["run", "--vars", "simple: abc"])
        self.assertEqual(len(results), 1)
        results = self.run_dbt(["test", "--vars", "simple: abc"])
        self.assertEqual(len(results), 1)

    @use_profile('postgres')
    def test__cli_vars_longer(self):
        self.use_profile('postgres')
        self.use_default_project()

        results = self.run_dbt(["run", "--vars", "{simple: abc, unused: def}"])
        self.assertEqual(len(results), 1)
        results = self.run_dbt(["test", "--vars", "{simple: abc, unused: def}"])
        self.assertEqual(len(results), 1)
