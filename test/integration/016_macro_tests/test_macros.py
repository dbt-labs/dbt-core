from test.integration.base import DBTIntegrationTest, use_profile

import dbt.exceptions
import pytest


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
                    'git': 'https://github.com/dbt-labs/dbt-integration-project',
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

    @use_profile('postgres')
    def test_postgres_invalid_macro(self):
        with pytest.raises(RuntimeError):
            self.run_dbt(["run"])


class TestAdapterMacroNoDestination(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_macros_016"

    @property
    def models(self):
        return "fail-missing-macro-models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ["no-default-macros"]
        }

    @use_profile('postgres')
    def test_postgres_invalid_macro(self):
        with pytest.raises(dbt.exceptions.CompilationException) as exc:
            self.run_dbt(['run'])

        assert "In dispatch: No macro named 'dispatch_to_nowhere' found" in str(exc.value)


class TestMacroOverrideBuiltin(DBTIntegrationTest):
    @property
    def schema(self):
        return "test_macros_016"

    @property
    def models(self):
        return 'override-get-columns-models'

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['override-get-columns-macros'],
        }

    @use_profile('postgres')
    def test_postgres_overrides(self):
        # the first time, the model doesn't exist
        self.run_dbt()
        self.run_dbt()


class TestDispatchMacroOverrideBuiltin(TestMacroOverrideBuiltin):
    # test the same functionality as above, but this time,
    # dbt.get_columns_in_relation will dispatch to a default__ macro
    # from an installed package, per dispatch config search_order

    @property
    def project_config(self):
        return {
            "config-version": 2,
            "dispatch": [
                {
                    "macro_namespace": "dbt",
                    "search_order": ["test", "package_macro_overrides", "dbt"],
                }
            ],
        }
        
    @property
    def packages_config(self):
        return {
            'packages': [
                {
                    "local": "./package_macro_overrides",
                },
            ]
        }

    @use_profile('postgres')
    def test_postgres_overrides(self):
        self.run_dbt(["deps"])
        super().test_postgres_overrides()


class TestAdapterMacroDeprecated(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_macros_016"

    @property
    def models(self):
        return "deprecated-adapter-macro-models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ["deprecated-adapter-macro"]
        }

    @use_profile('postgres')
    def test_postgres_invalid_macro(self):
        with pytest.raises(dbt.exceptions.CompilationException) as exc:
            self.run_dbt(['run'])

        assert 'The "adapter_macro" macro has been deprecated' in str(exc.value)
