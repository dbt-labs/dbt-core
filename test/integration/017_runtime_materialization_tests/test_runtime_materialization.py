from test.integration.base import DBTIntegrationTest, use_profile

class TestRuntimeMaterialization(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_dbt(['seed'])

    @property
    def project_config(self):
        return {"data-paths": ["test/integration/017_runtime_materialization_tests/data"]}

    @property
    def schema(self):
        return "runtime_materialization_017"

    @property
    def models(self):
        return "test/integration/017_runtime_materialization_tests/models"

    @use_profile('postgres')
    def test_postgres_full_refresh(self):
        # initial full-refresh should have no effect
        results = self.run_dbt(['run', '--full-refresh'])
        self.assertEqual(len(results), 3)

        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")

        # adds one record to the incremental model. full-refresh should truncate then re-run
        self.run_sql_file("test/integration/017_runtime_materialization_tests/invalidate_incremental.sql")
        results = self.run_dbt(['run', '--full-refresh'])
        self.assertEqual(len(results), 3)
        self.assertTablesEqual("seed","incremental")

        self.run_sql_file("test/integration/017_runtime_materialization_tests/update.sql")

        results = self.run_dbt(['run', '--full-refresh'])
        self.assertEqual(len(results), 3)

        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")

    @use_profile('postgres')
    def test_postgres_non_destructive(self):
        results = self.run_dbt(['run', '--non-destructive'])
        self.assertEqual(len(results), 3)

        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")
        self.assertTableDoesNotExist('dependent_view')

        self.run_sql_file("test/integration/017_runtime_materialization_tests/update.sql")

        results = self.run_dbt(['run', '--non-destructive'])
        self.assertEqual(len(results), 3)

        self.assertTableDoesExist('dependent_view')
        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")

    @use_profile('postgres')
    def test_postgres_full_refresh_and_non_destructive(self):
        results = self.run_dbt(['run', '--full-refresh', '--non-destructive'])
        self.assertEqual(len(results), 3)

        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")
        self.assertTableDoesNotExist('dependent_view')

        self.run_sql_file("test/integration/017_runtime_materialization_tests/invalidate_incremental.sql")
        self.run_sql_file("test/integration/017_runtime_materialization_tests/update.sql")

        results = self.run_dbt(['run', '--full-refresh', '--non-destructive'])
        self.assertEqual(len(results), 3)

        self.assertTableDoesExist('dependent_view')
        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")

    @use_profile('postgres')
    def test_postgres_delete__dbt_tmp_relation(self):
        # This creates a __dbt_tmp view - make sure it doesn't interfere with the dbt run
        self.run_sql_file("test/integration/017_runtime_materialization_tests/create_view__dbt_tmp.sql")
        results = self.run_dbt(['run', '--model', 'view'])
        self.assertEqual(len(results), 1)

        self.assertTableDoesNotExist('view__dbt_tmp')
        self.assertTablesEqual("seed","view")


    @use_profile('snowflake')
    def test_snowflake_backup_different_type(self):
        self.run_sql_file(
            'test/integration/017_runtime_materialization_tests/create_backup_and_original.sql'
        )
        results = self.run_dbt(['run', '--model', 'materialized'])
        self.assertEqual(len(results), 1)

        self.assertTableDoesNotExist('materialized__dbt_tmp')
        self.assertTableDoesNotExist('materialized__dbt_backup')
        self.assertTablesEqual("seed", "materialized")
