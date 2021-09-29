from tests.integration.base import DBTIntegrationTest, use_profile

from dbt import deprecations
import dbt.exceptions


class BaseTestDeprecations(DBTIntegrationTest):
    def setUp(self):
        super().setUp()
        deprecations.reset_deprecations()

    @property
    def schema(self):
        return "deprecation_test"

    @staticmethod
    def dir(path):
        return path.lstrip("/")


class TestAdapterMacroDeprecation(BaseTestDeprecations):
    @property
    def models(self):
        return self.dir('adapter-macro-models')

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': [self.dir('adapter-macro-macros')]
        }

    @use_profile('bigquery')
    def test_bigquery_adapter_macro(self):
        self.assertEqual(deprecations.active_deprecations, set())
        # picked up the default -> error
        with self.assertRaises(dbt.exceptions.CompilationException) as exc:
            self.run_dbt(expect_pass=False)
        exc_str = ' '.join(str(exc.exception).split())  # flatten all whitespace
        assert 'not allowed' in exc_str  # we saw the default macro


class TestAdapterMacroDeprecationPackages(BaseTestDeprecations):
    @property
    def models(self):
        return self.dir('adapter-macro-models-package')

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': [self.dir('adapter-macro-macros')]
        }

    @use_profile('bigquery')
    def test_bigquery_adapter_macro_pkg(self):
        self.assertEqual(deprecations.active_deprecations, set())
        # picked up the default -> error
        with self.assertRaises(dbt.exceptions.CompilationException) as exc:
            self.run_dbt(expect_pass=False)
        exc_str = ' '.join(str(exc.exception).split())  # flatten all whitespace
        assert 'not allowed' in exc_str  # we saw the default macro
