from test.integration.base import DBTIntegrationTest, use_profile

class TestSimpleReference(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_reference_003"

    @property
    def models(self):
        return "test/integration/003_simple_reference_test/models"

    @use_profile('postgres')
    def test__postgres__simple_reference(self):
        self.use_default_project()
        self.run_sql_file(
            "test/integration/003_simple_reference_test/seed.sql")

        results = self.run_dbt()
        # ephemeral_copy doesn't show up in results
        self.assertEqual(len(results),  7)

        # Copies should match
        self.assertTablesEqual("seed","incremental_copy")
        self.assertTablesEqual("seed","materialized_copy")
        self.assertTablesEqual("seed","view_copy")

        # Summaries should match
        self.assertTablesEqual("summary_expected","incremental_summary")
        self.assertTablesEqual("summary_expected","materialized_summary")
        self.assertTablesEqual("summary_expected","view_summary")
        self.assertTablesEqual("summary_expected","ephemeral_summary")

        self.run_sql_file("test/integration/003_simple_reference_test/update.sql")

        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        # Copies should match
        self.assertTablesEqual("seed","incremental_copy")
        self.assertTablesEqual("seed","materialized_copy")
        self.assertTablesEqual("seed","view_copy")

        # Summaries should match
        self.assertTablesEqual("summary_expected","incremental_summary")
        self.assertTablesEqual("summary_expected","materialized_summary")
        self.assertTablesEqual("summary_expected","view_summary")
        self.assertTablesEqual("summary_expected","ephemeral_summary")

    @use_profile('snowflake')
    def test__snowflake__simple_reference(self):
        self.use_default_project()
        self.run_sql_file("test/integration/003_simple_reference_test/seed.sql")

        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        # Copies should match
        self.assertManyTablesEqual(
            ["SEED", "INCREMENTAL_COPY", "MATERIALIZED_COPY", "VIEW_COPY"],
            ["SUMMARY_EXPECTED", "INCREMENTAL_SUMMARY", "MATERIALIZED_SUMMARY", "VIEW_SUMMARY", "EPHEMERAL_SUMMARY"]
        )

        self.run_sql_file(
            "test/integration/003_simple_reference_test/update.sql")

        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        self.assertManyTablesEqual(
            ["SEED", "INCREMENTAL_COPY", "MATERIALIZED_COPY", "VIEW_COPY"],
            ["SUMMARY_EXPECTED", "INCREMENTAL_SUMMARY", "MATERIALIZED_SUMMARY", "VIEW_SUMMARY", "EPHEMERAL_SUMMARY"]
        )

    @use_profile('postgres')
    def test__postgres__simple_reference_with_models(self):
        self.use_default_project()
        self.run_sql_file("test/integration/003_simple_reference_test/seed.sql")

        # Run materialized_copy, ephemeral_copy, and their dependents
        # ephemeral_copy should not actually be materialized b/c it is ephemeral
        results = self.run_dbt(
            ['run', '--models', 'materialized_copy', 'ephemeral_copy']
        )
        self.assertEqual(len(results),  1)

        # Copies should match
        self.assertTablesEqual("seed","materialized_copy")

        created_models = self.get_models_in_schema()
        self.assertTrue('materialized_copy' in created_models)

    @use_profile('postgres')
    def test__postgres__simple_reference_with_models_and_children(self):
        self.use_default_project()
        self.run_sql_file("test/integration/003_simple_reference_test/seed.sql")

        # Run materialized_copy, ephemeral_copy, and their dependents
        # ephemeral_copy should not actually be materialized b/c it is ephemeral
        # the dependent ephemeral_summary, however, should be materialized as a table
        results = self.run_dbt(
            ['run', '--models', 'materialized_copy+', 'ephemeral_copy+']
        )
        self.assertEqual(len(results),  3)

        # Copies should match
        self.assertTablesEqual("seed","materialized_copy")

        # Summaries should match
        self.assertTablesEqual("summary_expected","materialized_summary")
        self.assertTablesEqual("summary_expected","ephemeral_summary")

        created_models = self.get_models_in_schema()

        self.assertFalse('incremental_copy' in created_models)
        self.assertFalse('incremental_summary' in created_models)
        self.assertFalse('view_copy' in created_models)
        self.assertFalse('view_summary' in created_models)

        # make sure this wasn't errantly materialized
        self.assertFalse('ephemeral_copy' in created_models)

        self.assertTrue('materialized_copy' in created_models)
        self.assertTrue('materialized_summary' in created_models)
        self.assertEqual(created_models['materialized_copy'], 'table')
        self.assertEqual(created_models['materialized_summary'], 'table')

        self.assertTrue('ephemeral_summary' in created_models)
        self.assertEqual(created_models['ephemeral_summary'], 'table')

    @use_profile('snowflake')
    def test__snowflake__simple_reference_with_models(self):
        self.use_default_project()
        self.run_sql_file("test/integration/003_simple_reference_test/seed.sql")

        # Run materialized_copy & ephemeral_copy
        # ephemeral_copy should not actually be materialized b/c it is ephemeral
        results = self.run_dbt(
            ['run', '--models', 'materialized_copy', 'ephemeral_copy']
        )
        self.assertEqual(len(results),  1)

        # Copies should match
        self.assertTablesEqual("SEED", "MATERIALIZED_COPY")

        created_models = self.get_models_in_schema()
        self.assertTrue('MATERIALIZED_COPY' in created_models)

    @use_profile('snowflake')
    def test__snowflake__simple_reference_with_models_and_children(self):
        self.use_default_project()
        self.run_sql_file("test/integration/003_simple_reference_test/seed.sql")

        # Run materialized_copy, ephemeral_copy, and their dependents
        # ephemeral_copy should not actually be materialized b/c it is ephemeral
        # the dependent ephemeral_summary, however, should be materialized as a table
        results = self.run_dbt(
            ['run', '--models', 'materialized_copy+', 'ephemeral_copy+']
        )
        self.assertEqual(len(results),  3)

        # Copies should match
        self.assertManyTablesEqual(
            ["SEED", "MATERIALIZED_COPY"],
            ["SUMMARY_EXPECTED", "MATERIALIZED_SUMMARY", "EPHEMERAL_SUMMARY"]
        )

        created_models = self.get_models_in_schema()

        self.assertFalse('INCREMENTAL_COPY' in created_models)
        self.assertFalse('INCREMENTAL_SUMMARY' in created_models)
        self.assertFalse('VIEW_COPY' in created_models)
        self.assertFalse('VIEW_SUMMARY' in created_models)

        # make sure this wasn't errantly materialized
        self.assertFalse('EPHEMERAL_COPY' in created_models)

        self.assertTrue('MATERIALIZED_COPY' in created_models)
        self.assertTrue('MATERIALIZED_SUMMARY' in created_models)
        self.assertEqual(created_models['MATERIALIZED_COPY'], 'table')
        self.assertEqual(created_models['MATERIALIZED_SUMMARY'], 'table')

        self.assertTrue('EPHEMERAL_SUMMARY' in created_models)
        self.assertEqual(created_models['EPHEMERAL_SUMMARY'], 'table')
