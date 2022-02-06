from test.integration.base import DBTIntegrationTest, use_profile


class TestVarcharWidening(DBTIntegrationTest):
    @property
    def schema(self):
        return "varchar_widening_002"

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test__postgres__varchar_widening(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt()
        self.assertEqual(len(results),  2)

        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")

        self.run_sql_file("update.sql")

        results = self.run_dbt()
        self.assertEqual(len(results),  2)

        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")
