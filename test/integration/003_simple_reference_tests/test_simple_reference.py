import os

from dbt.exceptions import CompilationException

from test.integration.base import DBTIntegrationTest, use_profile


class TestSimpleReference(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_reference_003"

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

    @use_profile('postgres')
    def test__postgres__simple_reference(self):

        results = self.run_dbt()
        # ephemeral_copy doesn't show up in results
        self.assertEqual(len(results),  8)

        # Copies should match
        self.assertTablesEqual("seed","incremental_copy")
        self.assertTablesEqual("seed","materialized_copy")
        self.assertTablesEqual("seed","view_copy")

        # Summaries should match
        self.assertTablesEqual("summary_expected","incremental_summary")
        self.assertTablesEqual("summary_expected","materialized_summary")
        self.assertTablesEqual("summary_expected","view_summary")
        self.assertTablesEqual("summary_expected","ephemeral_summary")
        self.assertTablesEqual("summary_expected","view_using_ref")

        self.run_sql_file("update.sql")

        results = self.run_dbt()
        self.assertEqual(len(results),  8)

        # Copies should match
        self.assertTablesEqual("seed","incremental_copy")
        self.assertTablesEqual("seed","materialized_copy")
        self.assertTablesEqual("seed","view_copy")

        # Summaries should match
        self.assertTablesEqual("summary_expected","incremental_summary")
        self.assertTablesEqual("summary_expected","materialized_summary")
        self.assertTablesEqual("summary_expected","view_summary")
        self.assertTablesEqual("summary_expected","ephemeral_summary")
        self.assertTablesEqual("summary_expected","view_using_ref")

    @use_profile('postgres')
    def test__postgres__simple_reference_with_models(self):

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


class TestErrorReference(DBTIntegrationTest):
    @property
    def schema(self):
        return "simple_reference_003"

    @property
    def models(self):
        return "invalid-models"

    @use_profile('postgres')
    def test_postgres_undefined_value(self):
        with self.assertRaises(CompilationException) as exc:
            self.run_dbt(['compile'])
        path = os.path.join('invalid-models', 'descendant.sql')
        self.assertIn(path, str(exc.exception))
