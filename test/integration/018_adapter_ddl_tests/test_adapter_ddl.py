from test.integration.base import DBTIntegrationTest, use_profile

class TestAdapterDDL(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("test/integration/018_adapter_ddl_tests/seed.sql")

    @property
    def schema(self):
        return "adaper_ddl_018"

    @property
    def models(self):
        return "test/integration/018_adapter_ddl_tests/models"

    @use_profile('postgres')
    def test_sort_and_dist_keys_are_nops_on_postgres(self):
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed","materialized")
