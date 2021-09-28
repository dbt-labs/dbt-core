from tests.integration.base import DBTIntegrationTest, use_profile


class TestVarcharWidening(DBTIntegrationTest):
    @property
    def schema(self):
        return "varchar_widening"

    @property
    def models(self):
        return "models"

    @use_profile('snowflake')
    def test__snowflake__varchar_widening(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt()
        self.assertEqual(len(results),  2)

        self.assertManyTablesEqual(["SEED", "INCREMENTAL", "MATERIALIZED"])

        self.run_sql_file("update.sql")

        results = self.run_dbt()
        self.assertEqual(len(results),  2)

        self.assertManyTablesEqual(["SEED", "INCREMENTAL", "MATERIALIZED"])
