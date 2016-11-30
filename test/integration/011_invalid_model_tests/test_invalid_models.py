from test.integration.base import DBTIntegrationTest

class TestInvalidModels(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("test/integration/011_invalid_model_tests/seed.sql")

    @property
    def schema(self):
        return "invalid_models_011"

    @property
    def models(self):
        return "test/integration/011_invalid_model_tests/models"

    def test_view_with_incremental_attributes(self):
        self.run_dbt()
