from test.integration.base import DBTIntegrationTest

class TestSimpleDependency(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("test/integration/003_simple_dependency/seed.sql")

    @property
    def schema(self):
        return "simple_dependency_003"

    @property
    def models(self):
        return "test/integration/003_simple_dependency/models"

    def test_simple_dependency(self):
        self.run_dbt()

        # Copies should match
        self.assertTablesEqual("seed","incremental_copy")
        self.assertTablesEqual("seed","materialized_copy")
        self.assertTablesEqual("seed","view_copy")

        # Summaries should match
        self.assertTablesEqual("summary_expected","incremental_summary")
        self.assertTablesEqual("summary_expected","materialized_summary")
        self.assertTablesEqual("summary_expected","view_summary")
        self.assertTablesEqual("summary_expected","ephemeral_summary")

        self.run_sql_file("test/integration/003_simple_dependency/update.sql")

        self.run_dbt()

        # Copies should match
        self.assertTablesEqual("seed","incremental_copy")
        self.assertTablesEqual("seed","materialized_copy")
        self.assertTablesEqual("seed","view_copy")

        # Summaries should match
        self.assertTablesEqual("summary_expected","incremental_summary")
        self.assertTablesEqual("summary_expected","materialized_summary")
        self.assertTablesEqual("summary_expected","view_summary")
        self.assertTablesEqual("summary_expected","ephemeral_summary")
