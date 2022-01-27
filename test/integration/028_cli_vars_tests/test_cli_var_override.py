from test.integration.base import DBTIntegrationTest, use_profile


class TestCLIVarOverride(DBTIntegrationTest):
    @property
    def schema(self):
        return "cli_vars_028"

    @property
    def models(self):
        return "models_override"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'vars': {
                'required': 'present',
            },
        }

    @use_profile('postgres')
    def test__postgres_overriden_vars_global(self):
        self.use_default_project()
        self.use_profile('postgres')

        # This should be "override"
        self.run_dbt(["run", "--vars", "{required: override}"])
        self.run_dbt(["test"])


class TestCLIVarOverridePorject(DBTIntegrationTest):
    @property
    def schema(self):
        return "cli_vars_028"

    @property
    def models(self):
        return "models_override"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'vars': {
                'test': {
                    'required': 'present',
                },
            },
        }

    @use_profile('postgres')
    def test__postgres_overriden_vars_project_level(self):

        # This should be "override"
        self.run_dbt(["run", "--vars", "{required: override}"])
        self.run_dbt(["test"])
