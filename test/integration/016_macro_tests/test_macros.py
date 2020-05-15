from test.integration.base import DBTIntegrationTest, use_profile


class TestMacros(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "test_macros_016"

    @property
    def models(self):
        return "models"

    @property
    def packages_config(self):
        return {
            'packages': [
                {
                    'git': 'https://github.com/fishtown-analytics/dbt-integration-project',
                    'revision': 'dbt/0.17.0',
                },
            ]
        }

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'vars': {
                'test': {
                    'test': 'DUMMY',
                },
            },
            "macro-paths": ["macros"],
        }

    @use_profile('postgres')
    def test_postgres_working_macros(self):
        self.run_dbt(["deps"])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 6)

        self.assertTablesEqual("expected_dep_macro", "dep_macro")
        self.assertTablesEqual("expected_local_macro", "local_macro")


class TestInvalidMacros(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

    @property
    def schema(self):
        return "test_macros_016"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ["bad-macros"]
        }

    @use_profile('postgres')
    def test_postgres_invalid_macro(self):

        try:
            self.run_dbt(["run"], expect_pass=False)
            self.assertTrue(False,
                            'compiling bad macro should raise a runtime error')

        except RuntimeError:
            pass


class TestMisusedMacros(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

    @property
    def schema(self):
        return "test_macros_016"

    @property
    def models(self):
        return "bad-models"

    @property
    def packages_config(self):
        return {
            'packages': [
                {
                    'git': 'https://github.com/fishtown-analytics/dbt-integration-project',
                    'revision': 'dbt/0.17.0',
                }
            ]
        }

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ["macros"],
        }

    # TODO: compilation no longer exists, so while the model calling this macro
    # fails, it does not raise a runtime exception. change this test to verify
    # that the model finished with ERROR state.
    #
    # @use_profile('postgres')
    # def test_working_macros(self):
    #     self.run_dbt(["deps"])

    #     try:
    #         self.run_dbt(["run"])
    #         self.assertTrue(False, 'invoked a package macro from global scope')
    #     except RuntimeError:
    #         pass
