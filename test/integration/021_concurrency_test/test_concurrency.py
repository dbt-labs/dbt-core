from test.integration.base import DBTIntegrationTest, use_profile


class TestConcurrency(DBTIntegrationTest):
    @property
    def schema(self):
        return "concurrency_021"

    @property
    def models(self):
        return "test/integration/021_concurrency_test/models"

    @use_profile('postgres')
    def test__postgres__concurrency(self):
        self.run_sql_file("test/integration/021_concurrency_test/seed.sql")

        results = self.run_dbt(expect_pass=False)
        self.assertEqual(len(results), 7)

        self.assertTablesEqual("seed", "view_model")
        self.assertTablesEqual("seed", "dep")
        self.assertTablesEqual("seed", "table_a")
        self.assertTablesEqual("seed", "table_b")
        self.assertTableDoesNotExist("invalid")
        self.assertTableDoesNotExist("skip")

        self.run_sql_file("test/integration/021_concurrency_test/update.sql")

        results = self.run_dbt(expect_pass=False)
        self.assertEqual(len(results), 7)

        self.assertTablesEqual("seed", "view_model")
        self.assertTablesEqual("seed", "dep")
        self.assertTablesEqual("seed", "table_a")
        self.assertTablesEqual("seed", "table_b")
        self.assertTableDoesNotExist("invalid")
        self.assertTableDoesNotExist("skip")

    @use_profile('snowflake')
    def test__snowflake__concurrency(self):
        self.run_sql_file("test/integration/021_concurrency_test/seed.sql")

        results = self.run_dbt(expect_pass=False)
        self.assertEqual(len(results), 7)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "DEP", "TABLE_A", "TABLE_B"])

        self.run_sql_file("test/integration/021_concurrency_test/update.sql")

        results = self.run_dbt(expect_pass=False)
        self.assertEqual(len(results), 7)

        self.assertManyTablesEqual(["SEED", "VIEW_MODEL", "DEP", "TABLE_A", "TABLE_B"])
