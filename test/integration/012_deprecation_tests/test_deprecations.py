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


class TestDbtProjectYamlV1Deprecation(BaseTestDeprecations):
    @property
    def models(self):
        return 'boring-models'

    @property
    def project_config(self):
        return {'config-version': 1}

    @use_profile('postgres')
    def test_postgres_project_deprecations_fail(self):
        with self.assertRaises(dbt.exceptions.CompilationException) as exc:
            self.run_dbt(strict=True)

        exc_str = ' '.join(str(exc.exception).split())  # flatten all whitespace
        self.assertIn('Support for the existing version 1 format will be removed', exc_str)

    @use_profile('postgres')
    def test_postgres_project_deprecations(self):
        self.assertEqual(deprecations.active_deprecations, set())
        self.run_dbt(strict=False)
        expected = {'dbt-project-yaml-v1'}
        self.assertEqual(expected, deprecations.active_deprecations)
