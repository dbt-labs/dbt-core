from test.integration.base import DBTIntegrationTest, use_profile


class BaseTestSimpleDependencyWithConfigs(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "simple_dependency_006"

    @property
    def models(self):
        return "models"


class TestSimpleDependencyWithConfigs(BaseTestSimpleDependencyWithConfigs):
    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'git': 'https://github.com/dbt-labs/dbt-integration-project',
                    'revision': 'with-configs-0.17.0',
                },
            ]
        }

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'vars': {
                'dbt_integration_project': {
                    'bool_config': True
                },
            },
        }

    @use_profile('postgres')
    def test_postgres_simple_dependency(self):
        self.run_dbt(["deps"])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  5)

        self.assertTablesEqual('seed_config_expected_1', "config")
        self.assertTablesEqual("seed", "table_model")
        self.assertTablesEqual("seed", "view_model")
        self.assertTablesEqual("seed", "incremental")


class TestSimpleDependencyWithOverriddenConfigs(BaseTestSimpleDependencyWithConfigs):

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'git': 'https://github.com/dbt-labs/dbt-integration-project',
                    'revision': 'with-configs-0.17.0',
                },
            ]
        }

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "vars": {
                # project-level configs
                "dbt_integration_project": {
                    "config_1": "abc",
                    "config_2": "def",
                    "bool_config": True
                },
            },
        }

    @use_profile('postgres')
    def test_postgres_simple_dependency(self):
        self.run_dbt(["deps"])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  5)

        self.assertTablesEqual('seed_config_expected_2', "config")
        self.assertTablesEqual("seed", "table_model")
        self.assertTablesEqual("seed", "view_model")
        self.assertTablesEqual("seed", "incremental")
