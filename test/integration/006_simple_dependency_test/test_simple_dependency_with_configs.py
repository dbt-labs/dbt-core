from test.integration.base import DBTIntegrationTest, use_profile

class BaseTestSimpleDependencyWithConfigs(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("test/integration/006_simple_dependency_test/seed.sql")

    @property
    def schema(self):
        return "simple_dependency_006"

    @property
    def models(self):
        return "test/integration/006_simple_dependency_test/models"

class TestSimpleDependencyWithConfigs(BaseTestSimpleDependencyWithConfigs):
    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'git': 'https://github.com/fishtown-analytics/dbt-integration-project',
                    'revision': 'with-configs',
                },
            ]
        }

    @property
    def project_config(self):
        return {
            "models": {
                "dbt_integration_project": {
                    'vars': {
                        'bool_config': True
                    }
                }

            },
        }

    @use_profile('postgres')
    def test_simple_dependency(self):
        self.run_dbt(["deps"])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  5)

        self.assertTablesEqual('seed_config_expected_1',"config")
        self.assertTablesEqual("seed","table_model")
        self.assertTablesEqual("seed","view_model")
        self.assertTablesEqual("seed","incremental")


class TestSimpleDependencyWithOverriddenConfigs(BaseTestSimpleDependencyWithConfigs):

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'git': 'https://github.com/fishtown-analytics/dbt-integration-project',
                    'revision': 'with-configs',
                },
            ]
        }

    @property
    def project_config(self):
        return {
            "models": {
                # project-level configs
                "dbt_integration_project": {
                    "vars": {
                        "config_1": "abc",
                        "config_2": "def",
                        "bool_config": True

                    }
                }

            },
        }


    @use_profile('postgres')
    def test_simple_dependency(self):
        self.run_dbt(["deps"])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  5)

        self.assertTablesEqual('seed_config_expected_2',"config")
        self.assertTablesEqual("seed","table_model")
        self.assertTablesEqual("seed","view_model")
        self.assertTablesEqual("seed","incremental")



class TestSimpleDependencyWithModelSpecificOverriddenConfigs(BaseTestSimpleDependencyWithConfigs):

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'git': 'https://github.com/fishtown-analytics/dbt-integration-project',
                    'revision': 'with-configs',
                },
            ]
        }

    @property
    def project_config(self):
        return {
            "models": {
                "dbt_integration_project": {
                    "config": {
                        # model-level configs
                        "vars": {
                            "config_1": "ghi",
                            "config_2": "jkl",
                            "bool_config": True,
                        }
                    }
                }
            },
        }


    @use_profile('postgres')
    def test_simple_dependency(self):
        self.use_default_project()

        self.run_dbt(["deps"])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  5)

        self.assertTablesEqual('seed_config_expected_3',"config")
        self.assertTablesEqual("seed","table_model")
        self.assertTablesEqual("seed","view_model")
        self.assertTablesEqual("seed","incremental")


class TestSimpleDependencyWithModelSpecificOverriddenConfigsAndMaterializations(BaseTestSimpleDependencyWithConfigs):

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'git': 'https://github.com/fishtown-analytics/dbt-integration-project',
                    'revision': 'with-configs',
                },
            ]
        }

    @property
    def project_config(self):
        return {
            "models": {
                "dbt_integration_project": {
                    # disable config model, but supply vars
                    "config": {
                        "enabled": False,
                        "vars": {
                            "config_1": "ghi",
                            "config_2": "jkl",
                            "bool_config": True

                        }
                    },
                    # disable the table model
                    "table_model": {
                        "enabled": False,
                    },
                    # override materialization settings
                    "view_model": {
                        "materialized": "table"
                    }
                }

            },
        }


    @use_profile('postgres')
    def test_simple_dependency(self):
        self.run_dbt(["deps"])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  3)

        self.assertTablesEqual("seed","view_model")
        self.assertTablesEqual("seed","incremental")


        created_models = self.get_models_in_schema()

        # config, table are disabled
        self.assertFalse('config' in created_models)
        self.assertFalse('table_model' in created_models)

        self.assertTrue('view_model' in created_models)
        self.assertEqual(created_models['view_model'], 'table')

        self.assertTrue('incremental' in created_models)
        self.assertEqual(created_models['incremental'], 'table')
