from test.integration.base import DBTIntegrationTest, use_profile

from dbt import deprecations
import dbt.exceptions


class BaseTestDeprecations(DBTIntegrationTest):
    def setUp(self):
        super().setUp()
        deprecations.reset_deprecations()

    @property
    def schema(self):
        return "deprecation_test_012"

    @staticmethod
    def dir(path):
        return path.lstrip("/")


class TestDeprecations(BaseTestDeprecations):
    @property
    def models(self):
        return self.dir("models")

    @use_profile('postgres')
    def test_postgres_deprecations_fail(self):
        self.run_dbt(['--warn-error', 'run'], expect_pass=False)

    @use_profile('postgres')
    def test_postgres_deprecations(self):
        self.assertEqual(deprecations.active_deprecations, set())
        self.run_dbt()
        expected = {'adapter:already_exists'}
        self.assertEqual(expected, deprecations.active_deprecations)


class TestDispatchPackagesDeprecation(BaseTestDeprecations):
    @property
    def models(self):
        return self.dir('dispatch-models')

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": [self.dir('dispatch-macros')],
            "models": {
                "test": {
                    "alias_in_project": {
                        "alias": 'project_alias',
                    },
                    "alias_in_project_with_override": {
                        "alias": 'project_alias',
                    },
                }
            }
        }

    @use_profile('postgres')
    def test_postgres_adapter_macro(self):
        self.assertEqual(deprecations.active_deprecations, set())
        self.run_dbt()
        expected = {'dispatch-packages'}
        self.assertEqual(expected, deprecations.active_deprecations)

    @use_profile('postgres')
    def test_postgres_adapter_macro_fail(self):
        self.assertEqual(deprecations.active_deprecations, set())
        with self.assertRaises(dbt.exceptions.CompilationException) as exc:
            self.run_dbt(['--warn-error', 'run'])
        exc_str = ' '.join(str(exc.exception).split())  # flatten all whitespace
        assert 'Raised during dispatch for: string_literal' in exc_str


class TestPackageRedirectDeprecation(BaseTestDeprecations):
    @property
    def models(self):
        return self.dir('where-were-going-we-dont-need-models')

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'package': 'fishtown-analytics/dbt_utils',
                    'version': '0.7.0'
                }
            ]
        }
    
    @use_profile('postgres')
    def test_postgres_package_redirect(self):
        self.assertEqual(deprecations.active_deprecations, set())
        self.run_dbt(['deps'])
        expected = {'package-redirect'}
        self.assertEqual(expected, deprecations.active_deprecations)

    @use_profile('postgres')
    def test_postgres_package_redirect_fail(self):
        self.assertEqual(deprecations.active_deprecations, set())
        with self.assertRaises(dbt.exceptions.CompilationException) as exc:
            self.run_dbt(['--warn-error', 'deps'])
        exc_str = ' '.join(str(exc.exception).split())  # flatten all whitespace
        expected = "The `fishtown-analytics/dbt_utils` package is deprecated in favor of `dbt-labs/dbt_utils`"
        assert expected in exc_str