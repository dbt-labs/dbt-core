from dbt.exceptions import CompilationException
from test.integration.base import DBTIntegrationTest, use_profile


class TestDuplicateSourceEnabled(DBTIntegrationTest):

    @property
    def schema(self):
        return "duplicate_model_025"

    @property
    def models(self):
        return "models-source-dupes"

    @use_profile("postgres")
    def test_postgres_duplicate_source_enabled(self):
        message = "dbt found two sources with the name"
        try:
            self.run_dbt(["compile"])
            self.assertTrue(False, "dbt did not throw for duplicate sources")
        except CompilationException as e:
            self.assertTrue(message in str(
                e), "dbt did not throw the correct error message")
