from tests.integration.base import DBTIntegrationTest, use_profile


class TestConcurrency(DBTIntegrationTest):
    @property
    def schema(self):
        return "concurrency"

    @property
    def models(self):
        return "models"

    @use_profile('snowflake')
    def test__snowflake__concurrency(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(expect_pass=False)
        self.assertEqual(len(results), 7)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "DEP", "TABLE_A", "TABLE_B"])

        self.run_sql_file("update.sql")

        results = self.run_dbt(expect_pass=False)
        self.assertEqual(len(results), 7)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "DEP", "TABLE_A", "TABLE_B"])
