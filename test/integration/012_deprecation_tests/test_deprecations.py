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

    @property
    def models(self):
        return self.dir("models")


class TestDeprecations(BaseTestDeprecations):
    @use_profile('postgres')
    def test_postgres_deprecations_fail(self):
        self.run_dbt(strict=True, expect_pass=False)

    @use_profile('postgres')
    def test_postgres_deprecations(self):
        self.assertEqual(deprecations.active_deprecations, set())
        self.run_dbt(strict=False)
        expected = {'adapter:already_exists'}
        self.assertEqual(expected, deprecations.active_deprecations)


class TestMacroDeprecations(BaseTestDeprecations):
    @property
    def models(self):
        return self.dir('boring-models')

    @property
    def project_config(self):
        return {
            'macro-paths': [self.dir('deprecated-macros')],
        }

    @use_profile('postgres')
    def test_postgres_deprecations_fail(self):
        with self.assertRaises(dbt.exceptions.CompilationException):
            self.run_dbt(strict=True)

    @use_profile('postgres')
    def test_postgres_deprecations(self):
        self.assertEqual(deprecations.active_deprecations, set())
        self.run_dbt(strict=False)
        expected = {'generate-schema-name-single-arg'}
        self.assertEqual(expected, deprecations.active_deprecations)
