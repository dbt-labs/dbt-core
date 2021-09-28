import os

from dbt.exceptions import CompilationException

from tests.integration.base import DBTIntegrationTest, use_profile


class TestSimpleReference(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_reference"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'vars': {
                'test': {
                    'var_ref': '{{ ref("view_copy") }}',
                },
            },
        }

    def setUp(self):
        super().setUp()
        # self.use_default_config()
        self.run_sql_file("seed.sql")

    @use_profile('snowflake')
    def test__snowflake__simple_reference(self):

        results = self.run_dbt()
        self.assertEqual(len(results),  8)

        # Copies should match
        self.assertManyTablesEqual(
            ["SEED", "INCREMENTAL_COPY", "MATERIALIZED_COPY", "VIEW_COPY"],
            ["SUMMARY_EXPECTED", "INCREMENTAL_SUMMARY", "MATERIALIZED_SUMMARY", "VIEW_SUMMARY", "EPHEMERAL_SUMMARY"]
        )

        self.run_sql_file("update.sql")

        results = self.run_dbt()
        self.assertEqual(len(results),  8)

        self.assertManyTablesEqual(
            ["SEED", "INCREMENTAL_COPY", "MATERIALIZED_COPY", "VIEW_COPY"],
            ["SUMMARY_EXPECTED", "INCREMENTAL_SUMMARY", "MATERIALIZED_SUMMARY", "VIEW_SUMMARY", "EPHEMERAL_SUMMARY"]
        )

    @use_profile('snowflake')
    def test__snowflake__simple_reference_with_models(self):

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
