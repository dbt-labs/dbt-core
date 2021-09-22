from tests.integration.base import DBTIntegrationTest, use_profile

from dbt import deprecations


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

    @use_profile('redshift')
    def test_redshift_adapter_macro(self):
        self.assertEqual(deprecations.active_deprecations, set())
        # pick up the postgres macro
        self.run_dbt()
        expected = {'adapter-macro'}
        self.assertEqual(expected, deprecations.active_deprecations)


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

    @use_profile('redshift')
    def test_redshift_adapter_macro_pkg(self):
        self.assertEqual(deprecations.active_deprecations, set())
        # pick up the postgres macro
        self.assertEqual(deprecations.active_deprecations, set())
        self.run_dbt()
        expected = {'adapter-macro'}
        self.assertEqual(expected, deprecations.active_deprecations)
