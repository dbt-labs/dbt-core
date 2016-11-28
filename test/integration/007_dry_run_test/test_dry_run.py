from test.integration.base import DBTIntegrationTest

class TestDryRun(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("test/integration/007_dry_run_test/seed.sql")

    @property
    def schema(self):
        return "dry_run_007"

    @property
    def models(self):
        return "test/integration/007_dry_run_test/models"

    def test_dry_run(self):
        self.run_dbt(["run", '--dry'])

        created_models = self.get_models_in_schema()

        # this shouldn't create any models (besides seed created outside dbt)
        self.assertEqual(created_models, {"seed": "table"})
