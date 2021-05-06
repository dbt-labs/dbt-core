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


class TestModelsKeyMismatchDeprecation(BaseTestDeprecations):
    @property
    def models(self):
        return self.dir('models-key-mismatch')

    @use_profile('postgres')
    def test_postgres_deprecations_fail(self):
        # this should fail at compile_time
        with self.assertRaises(dbt.exceptions.CompilationException) as exc:
            self.run_dbt(strict=True)
        exc_str = ' '.join(str(exc.exception).split())  # flatten all whitespace
        self.assertIn('"seed" is a seed node, but it is specified in the models section', exc_str)

    @use_profile('postgres')
    def test_postgres_deprecations(self):
        self.assertEqual(deprecations.active_deprecations, set())
        self.run_dbt(strict=False)
        expected = {'models-key-mismatch'}
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
