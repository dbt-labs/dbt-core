from test.integration.base import DBTIntegrationTest, use_profile

from dbt.exceptions import ValidationException


class TestInvalidDisabledModels(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("test/integration/011_invalid_model_tests/seed.sql")

    @property
    def schema(self):
        return "invalid_models_011"

    @property
    def models(self):
        return "test/integration/011_invalid_model_tests/models-2"

    @use_profile('postgres')
    def test_view_with_incremental_attributes(self):

        try:
            self.run_dbt()
            # should throw
            self.assertTrue(False)
        except RuntimeError as e:
            self.assertTrue("enabled" in str(e))


class TestInvalidModelReference(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("test/integration/011_invalid_model_tests/seed.sql")

    @property
    def schema(self):
        return "invalid_models_011"

    @property
    def models(self):
        return "test/integration/011_invalid_model_tests/models-3"

    @use_profile('postgres')
    def test_view_with_incremental_attributes(self):

        try:
            self.run_dbt()
            # should throw
            self.assertTrue(False)
        except RuntimeError as e:
            self.assertTrue("which was not found" in str(e))
