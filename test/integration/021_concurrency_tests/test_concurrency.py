from test.integration.base import DBTIntegrationTest, use_profile


class TestConcurrency(DBTIntegrationTest):
    @property
    def schema(self):
        return "concurrency_021"

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test__postgres__concurrency(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(expect_pass=False)
        self.assertEqual(len(results), 7)

        self.assertTablesEqual("seed", "view_model")
        self.assertTablesEqual("seed", "dep")
        self.assertTablesEqual("seed", "table_a")
        self.assertTablesEqual("seed", "table_b")
        self.assertTableDoesNotExist("invalid")
        self.assertTableDoesNotExist("skip")

        self.run_sql_file("update.sql")

        results, output = self.run_dbt_and_capture(expect_pass=False)
        self.assertEqual(len(results), 7)

        self.assertTablesEqual("seed", "view_model")
        self.assertTablesEqual("seed", "dep")
        self.assertTablesEqual("seed", "table_a")
        self.assertTablesEqual("seed", "table_b")
        self.assertTableDoesNotExist("invalid")
        self.assertTableDoesNotExist("skip")

        self.assertIn('PASS=5 WARN=0 ERROR=1 SKIP=1 TOTAL=7', output)
