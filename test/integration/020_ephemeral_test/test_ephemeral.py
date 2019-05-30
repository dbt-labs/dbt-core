from test.integration.base import DBTIntegrationTest, use_profile


class TestEphemeral(DBTIntegrationTest):
    @property
    def schema(self):
        return "ephemeral_020"

    @property
    def models(self):
        return "test/integration/020_ephemeral_test/models"

    @use_profile('postgres')
    def test__postgres(self):
        self.run_sql_file("test/integration/020_ephemeral_test/seed.sql")

        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        self.assertTablesEqual("seed", "dependent")
        self.assertTablesEqual("seed", "double_dependent")
        self.assertTablesEqual("seed", "super_dependent")

    @use_profile('snowflake')
    def test__snowflake(self):
        self.run_sql_file("test/integration/020_ephemeral_test/seed.sql")

        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        self.assertManyTablesEqual(
            ["SEED", "DEPENDENT", "DOUBLE_DEPENDENT", "SUPER_DEPENDENT"]
        )

class TestEphemeralErrorHandling(DBTIntegrationTest):
    @property
    def schema(self):
        return "ephemeral_020"

    @property
    def models(self):
        return "test/integration/020_ephemeral_test/ephemeral-errors"

    @use_profile('postgres')
    def test__postgres_upstream_error(self):
        self.run_sql_file("test/integration/020_ephemeral_test/seed.sql")

        results = self.run_dbt(expect_pass=False)
        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].error is not None)
