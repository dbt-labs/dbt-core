from test.integration.base import DBTIntegrationTest
import os

class TestCLIInvocation(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("test/integration/015_cli_invocation_tests/seed.sql")

    @property
    def schema(self):
        return "test_cli_invocation_015"

    @property
    def models(self):
        return "test/integration/015_cli_invocation_tests/models"

    def test_toplevel_dbt_run(self):
        self.run_dbt(['run'])
        self.assertTablesEqual("seed","model")

    def test_subdir_dbt_run(self):
        os.chdir(os.path.join(self.models, "subdir1"))

        self.run_dbt(['run'])
        self.assertTablesEqual("seed","model")
