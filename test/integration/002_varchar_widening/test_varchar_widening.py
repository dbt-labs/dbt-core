from test.integration.base import DBTIntegrationTest

class TestVarcharWidening(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("test/integration/002_varchar_widening/seed.sql")

    @property
    def schema(self):
        return "varchar_widening_002"

    @property
    def models(self):
        return "test/integration/002_varchar_widening/models"

    def test_varchar_widening(self):
        self.run_dbt()

        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")

        self.run_sql_file("test/integration/002_varchar_widening/update.sql")

        self.run_dbt()

        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")
