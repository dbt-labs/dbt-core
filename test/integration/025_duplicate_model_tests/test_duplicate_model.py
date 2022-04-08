from dbt.exceptions import CompilationException
from test.integration.base import DBTIntegrationTest, use_profile, get_manifest


class TestDuplicateModelEnabled(DBTIntegrationTest):

    @property
    def schema(self):
        return "duplicate_model_025"

    @property
    def models(self):
        return "models-1"

    @use_profile("postgres")
    def test_postgres_duplicate_model_enabled(self):
        message = "dbt found two models with the name"
        try:
            self.run_dbt(["run"])
            self.assertTrue(False, "dbt did not throw for duplicate models")
        except CompilationException as e:
            self.assertTrue(message in str(
                e), "dbt did not throw the correct error message")


class TestDuplicateModelDisabled(DBTIntegrationTest):

    @property
    def schema(self):
        return "duplicate_model_025"

    @property
    def models(self):
        return "models-2"

    @use_profile("postgres")
    def test_postgres_duplicate_model_disabled(self):
        try:
            results = self.run_dbt(["run"])
        except CompilationException:
            self.fail(
                "Compilation Exception raised on disabled model")
        self.assertEqual(len(results), 1)
        query = "select value from {schema}.model" \
                .format(schema=self.unique_schema())
        result = self.run_sql(query, fetch="one")[0]
        self.assertEqual(result, 1)

    @use_profile('postgres')
    def test_postgres_duplicate_model_disabled_partial_parsing(self):
        self.run_dbt(['clean'])
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 1)
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 1)
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 1)


class TestDuplicateModelEnabledAcrossPackages(DBTIntegrationTest):

    @property
    def schema(self):
        return "duplicate_model_025"

    @property
    def models(self):
        return "models-3"

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'local': 'local_dependency'
                }
            ]
        }

    @use_profile("postgres")
    def test_postgres_duplicate_model_enabled_across_packages(self):
        self.run_dbt(["deps"])
        message = "dbt found two models with the name"
        try:
            self.run_dbt(["run"])
            self.assertTrue(False, "dbt did not throw for duplicate models")
        except CompilationException as e:
            self.assertTrue(message in str(
                e), "dbt did not throw the correct error message")


class TestDuplicateModelDisabledAcrossPackages(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "duplicate_model_025"

    @property
    def models(self):
        return "models-4"

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'local': 'local_dependency'
                }
            ]
        }

    @use_profile("postgres")
    def test_postgres_duplicate_model_disabled_across_packages(self):
        self.run_dbt(["deps"])
        try:
            self.run_dbt(["run"])
        except CompilationException:
            self.fail(
                "Compilation Exception raised on disabled model")
        query = "select 1 from {schema}.table_model" \
                .format(schema=self.unique_schema())
        result = self.run_sql(query, fetch="one")[0]
        assert result == 1


class TestModelTestOverlap(DBTIntegrationTest):

    @property
    def schema(self):
        return "duplicate_model_025"

    @property
    def models(self):
        return "models-3"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'test-paths': [self.models],
        }

    @use_profile('postgres')
    def test_postgres_duplicate_test_model_paths(self):
        # this should be ok: test/model overlap is fine
        self.run_dbt(['compile'])
        self.run_dbt(['--partial-parse', 'compile'])
        self.run_dbt(['--partial-parse', 'compile'])


class TestMultipleDisabledModels(DBTIntegrationTest):

    @property
    def schema(self):
        return "duplicate_model_025"

    @property
    def models(self):
        return "models-5"

    @use_profile('postgres')
    def test_postgres_multiple_disabled_models(self):
        self.run_dbt(['run'])
        manifest = get_manifest()
        model_id = 'model.test.model_alt'
        self.assertIn(model_id, manifest.nodes)
