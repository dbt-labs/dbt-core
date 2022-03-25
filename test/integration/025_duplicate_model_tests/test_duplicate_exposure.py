from dbt.exceptions import CompilationException
from test.integration.base import DBTIntegrationTest, use_profile


class TestDuplicateExposure(DBTIntegrationTest):

    @property
    def schema(self):
        return "duplicate_exposure_025"

    @property
    def models(self):
        return "models-exposure-dupes"

    @use_profile("postgres")
    def test_postgres_duplicate_exposure(self):
        message = "dbt found two exposures with the name"
        try:
            self.run_dbt(["compile"])
            self.assertTrue(False, "dbt did not throw for duplicate exposures")
        except CompilationException as e:
            self.assertTrue(message in str(
                e), "dbt did not throw the correct error message")
