from dbt.exceptions import CompilationException
from test.integration.base import DBTIntegrationTest, use_profile


class TestDuplicateSchemaResource(DBTIntegrationTest):

    @property
    def schema(self):
        return "duplicate_resource_025"

    @property
    def models(self):
        return "models-naming-dupes"

    @use_profile("postgres")
    def test_postgres_duplicate_model_and_exposure(self):
        try:
            self.run_dbt(["compile"])
        except CompilationException:
            self.fail("Compilation Exception raised on model and "
                      "exposure with the same name")
