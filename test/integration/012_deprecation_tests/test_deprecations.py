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
        self.run_dbt(strict=True, expect_pass=False)

    @use_profile('postgres')
    def test_postgres_deprecations(self):
        self.assertEqual(deprecations.active_deprecations, set())
        self.run_dbt(strict=False)
        expected = {'adapter:already_exists'}
        self.assertEqual(expected, deprecations.active_deprecations)


class TestMaterializationReturnDeprecation(BaseTestDeprecations):
    @property
    def models(self):
        return self.dir('custom-models')

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': [self.dir('custom-materialization-macros')],
        }

    @use_profile('postgres')
    def test_postgres_deprecations_fail(self):
        # this should fail at runtime
        self.run_dbt(strict=True, expect_pass=False)

    @use_profile('postgres')
    def test_postgres_deprecations(self):
        self.assertEqual(deprecations.active_deprecations, set())
        self.run_dbt(strict=False)
        expected = {'materialization-return'}
        self.assertEqual(expected, deprecations.active_deprecations)


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

    @use_profile('postgres')
    def test_postgres_adapter_macro(self):
        self.assertEqual(deprecations.active_deprecations, set())
        self.run_dbt(strict=False)
        expected = {'adapter-macro'}
        self.assertEqual(expected, deprecations.active_deprecations)

    @use_profile('postgres')
    def test_postgres_adapter_macro_fail(self):
        self.assertEqual(deprecations.active_deprecations, set())
        with self.assertRaises(dbt.exceptions.CompilationException) as exc:
            self.run_dbt(strict=True)
        exc_str = ' '.join(str(exc.exception).split())  # flatten all whitespace
        assert 'The "adapter_macro" macro has been deprecated' in exc_str

    @use_profile('redshift')
    def test_redshift_adapter_macro(self):
        self.assertEqual(deprecations.active_deprecations, set())
        # pick up the postgres macro
        self.run_dbt(strict=False)
        expected = {'adapter-macro'}
        self.assertEqual(expected, deprecations.active_deprecations)
        
    @use_profile('bigquery')
    def test_bigquery_adapter_macro(self):
        self.assertEqual(deprecations.active_deprecations, set())
        # picked up the default -> error
        with self.assertRaises(dbt.exceptions.CompilationException) as exc:
            self.run_dbt(strict=False, expect_pass=False)
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

    @use_profile('postgres')
    def test_postgres_adapter_macro_pkg(self):
        self.assertEqual(deprecations.active_deprecations, set())
        self.run_dbt(strict=False)
        expected = {'adapter-macro'}
        self.assertEqual(expected, deprecations.active_deprecations)

    @use_profile('postgres')
    def test_postgres_adapter_macro_pkg_fail(self):
        self.assertEqual(deprecations.active_deprecations, set())
        with self.assertRaises(dbt.exceptions.CompilationException) as exc:
            self.run_dbt(strict=True)
        exc_str = ' '.join(str(exc.exception).split())  # flatten all whitespace
        assert 'The "adapter_macro" macro has been deprecated' in exc_str

    @use_profile('redshift')
    def test_redshift_adapter_macro_pkg(self):
        self.assertEqual(deprecations.active_deprecations, set())
        # pick up the postgres macro
        self.assertEqual(deprecations.active_deprecations, set())
        self.run_dbt(strict=False)
        expected = {'adapter-macro'}
        self.assertEqual(expected, deprecations.active_deprecations)

    @use_profile('bigquery')
    def test_bigquery_adapter_macro_pkg(self):
        self.assertEqual(deprecations.active_deprecations, set())
        # picked up the default -> error
        with self.assertRaises(dbt.exceptions.CompilationException) as exc:
            self.run_dbt(strict=False, expect_pass=False)
        exc_str = ' '.join(str(exc.exception).split())  # flatten all whitespace
        assert 'not allowed' in exc_str  # we saw the default macro


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
        self.run_dbt(strict=False)
        expected = {'dispatch-packages'}
        self.assertEqual(expected, deprecations.active_deprecations)

    @use_profile('postgres')
    def test_postgres_adapter_macro_fail(self):
        self.assertEqual(deprecations.active_deprecations, set())
        with self.assertRaises(dbt.exceptions.CompilationException) as exc:
            self.run_dbt(strict=True)
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
        self.run_dbt(['deps'], strict=False)
        expected = {'package-redirect'}
        self.assertEqual(expected, deprecations.active_deprecations)

    @use_profile('postgres')
    def test_postgres_package_redirect_fail(self):
        self.assertEqual(deprecations.active_deprecations, set())
        with self.assertRaises(dbt.exceptions.CompilationException) as exc:
            self.run_dbt(['deps'], strict=True)
        exc_str = ' '.join(str(exc.exception).split())  # flatten all whitespace
        expected = "The `fishtown-analytics/dbt_utils` package is deprecated in favor of `dbt-labs/dbt_utils`"
        assert expected in exc_str