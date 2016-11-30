from test.integration.base import DBTIntegrationTest

class TestSimpleCopy(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("test/integration/001_simple_copy_test/seed.sql")

    @property
    def schema(self):
        return "simple_copy_001"

    @property
    def models(self):
        return "test/integration/001_simple_copy_test/models"

    def test_simple_copy(self):
        self.run_dbt()

        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")

        self.run_sql_file("test/integration/001_simple_copy_test/update.sql")

        self.run_dbt()

        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")

    def test_dbt_doesnt_run_empty_models(self):
        self.run_dbt()

        models = self.get_models_in_schema()

        self.assertFalse('empty' in models.keys())
        self.assertFalse('disabled' in models.keys())
