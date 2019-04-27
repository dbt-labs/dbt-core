from dbt.exceptions import CompilationException
from test.integration.base import DBTIntegrationTest, use_profile


class TestDuplicateModelEnabled(DBTIntegrationTest):

    @property
    def schema(self):
        return "duplicate_model_025"

    @property
    def models(self):
        return "test/integration/025_duplicate_model_test/models-1"

    @property
    def profile_config(self):
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "postgres",
                        "threads": 1,
                        "host": self.database_host,
                        "port": 5432,
                        "user": "root",
                        "pass": "password",
                        "dbname": "dbt",
                        "schema": self.unique_schema()
                    },
                },
                "target": "dev"
            }
        }

    @use_profile("postgres")
    def test_duplicate_model_enabled(self):
        message = "dbt found two resources with the name"
        try:
            self.run_dbt(["run"])
            self.assertTrue(False, "dbt did not throw for duplicate models")
        except CompilationException as e:
            self.assertTrue(message in str(e), "dbt did not throw the correct error message")


class TestDuplicateModelDisabled(DBTIntegrationTest):

    @property
    def schema(self):
        return "duplicate_model_025"

    @property
    def models(self):
        return "test/integration/025_duplicate_model_test/models-2"

    @property
    def profile_config(self):
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "postgres",
                        "threads": 1,
                        "host": self.database_host,
                        "port": 5432,
                        "user": "root",
                        "pass": "password",
                        "dbname": "dbt",
                        "schema": self.unique_schema()
                    },
                },
                "target": "dev"
            }
        }

    @use_profile("postgres")
    def test_duplicate_model_disabled(self):
        try:
            results = self.run_dbt(["run"])
        except CompilationException:
            self.fail(
                "Compilation Exception raised on disabled model")
        self.assertEqual(len(results), 1)
        query = "select value from {schema}.model" \
                .format(schema=self.unique_schema())
        result = self.run_sql(query, fetch="one")[0]
        assert result == 1


class TestDuplicateModelEnabledAcrossPackages(DBTIntegrationTest):

    @property
    def schema(self):
        return "duplicate_model_025"

    @property
    def models(self):
        return "test/integration/025_duplicate_model_test/models-3"

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'git': 'https://github.com/fishtown-analytics/dbt-integration-project',
                    'revision': 'master',
                },
            ],
        }

    @use_profile("postgres")
    def test_duplicate_model_enabled_across_packages(self):
        self.run_dbt(["deps"])
        message = "dbt found two resources with the name"
        try:
            self.run_dbt(["run"])
            self.assertTrue(False, "dbt did not throw for duplicate models")
        except CompilationException as e:
            self.assertTrue(message in str(e), "dbt did not throw the correct error message")


class TestDuplicateModelDisabledAcrossPackages(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("test/integration/025_duplicate_model_test/seed.sql")

    @property
    def schema(self):
        return "duplicate_model_025"

    @property
    def models(self):
        return "test/integration/025_duplicate_model_test/models-4"

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'git': 'https://github.com/fishtown-analytics/dbt-integration-project',
                    'revision': 'master',
                },
            ],
        }

    @use_profile("postgres")
    def test_duplicate_model_disabled_across_packages(self):
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
