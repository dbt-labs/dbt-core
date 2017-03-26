from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest

class TestRuntimeMaterialization(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("test/integration/017_runtime_materialization_tests/seed.sql")

    @property
    def schema(self):
        return "runtime_materialization_017"

    @property
    def models(self):
        return "test/integration/017_runtime_materialization_tests/models"

    @attr(type='postgres')
    def test_full_refresh(self):
        # initial full-refresh should have no effect
        self.run_dbt(['run', '--full-refresh'])

        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")

        # adds one record to the incremental model. full-refresh should truncate then re-run
        self.run_sql_file("test/integration/017_runtime_materialization_tests/invalidate_incremental.sql")
        self.run_dbt(['run', '--full-refresh'])
        self.assertTablesEqual("seed","incremental")

        self.run_sql_file("test/integration/017_runtime_materialization_tests/update.sql")

        self.run_dbt(['run', '--full-refresh'])

        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")

    @attr(type='postgres')
    def test_non_destructive(self):
        self.run_dbt(['run', '--non-destructive'])

        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")
        self.assertTableDoesNotExist('dependent_view')

        self.run_sql_file("test/integration/017_runtime_materialization_tests/update.sql")

        self.run_dbt(['run', '--non-destructive'])

        self.assertTableDoesExist('dependent_view')
        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")

    @attr(type='postgres')
    def test_full_refresh_and_non_destructive(self):
        self.run_dbt(['run', '--full-refresh', '--non-destructive'])

        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")
        self.assertTableDoesNotExist('dependent_view')

        self.run_sql_file("test/integration/017_runtime_materialization_tests/invalidate_incremental.sql")
        self.run_sql_file("test/integration/017_runtime_materialization_tests/update.sql")

        self.run_dbt(['run', '--full-refresh', '--non-destructive'])

        self.assertTableDoesExist('dependent_view')
        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")
