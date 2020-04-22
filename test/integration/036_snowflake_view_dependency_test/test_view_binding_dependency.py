from test.integration.base import DBTIntegrationTest, use_profile

class TestSnowflakeLateBindingViewDependency(DBTIntegrationTest):

    @property
    def schema(self):
        return "snowflake_view_dependency_test_036"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'seeds': {
                'quote_columns': False,
            },
            'quoting': {
                'schema': False,
                'identifier': False
            }
        }

    """
    Snowflake views are not bound to the relations they select from. A Snowflake view
    can have entirely invalid SQL if, for example, the table it selects from is dropped
    and recreated with a different schema. In these scenarios, Snowflake will raise an error if:
    1) The view is queried
    2) The view is altered

    dbt's logic should avoid running these types of queries against views in situations
    where they _may_ have invalid definitions. These tests assert that views are handled
    correctly in various different scenarios
    """

    @use_profile('snowflake')
    def test__snowflake__changed_table_schema_for_downstream_view(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)

        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  2)
        self.assertManyTablesEqual(["PEOPLE", "BASE_TABLE", "DEPENDENT_MODEL"])

        # Change the schema of base_table, assert that dependent_model doesn't fail
        results = self.run_dbt(["run", "--vars", "{add_table_field: true, dependent_type: view}"])
        self.assertEqual(len(results),  2)
        self.assertManyTablesEqual(["BASE_TABLE", "DEPENDENT_MODEL"])

    """
    This test is similar to the one above, except the downstream model starts as a view, and
    then is changed to be a table. This checks that the table materialization does not
    errantly rename a view that might have an invalid definition, which would cause an error
    """
    @use_profile('snowflake')
    def test__snowflake__changed_table_schema_for_downstream_view_changed_to_table(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)

        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  2)
        self.assertManyTablesEqual(["PEOPLE", "BASE_TABLE", "DEPENDENT_MODEL"])

        expected_types = {
            'base_table': 'table',
            'dependent_model': 'view'
        }

        # ensure that the model actually was materialized as a table
        for result in results:
            node_name = result.node.name
            self.assertEqual(result.node.config.materialized, expected_types[node_name])

        results = self.run_dbt(["run", "--vars", "{add_table_field: true, dependent_type: table}"])
        self.assertEqual(len(results),  2)
        self.assertManyTablesEqual(["BASE_TABLE", "DEPENDENT_MODEL"])

        expected_types = {
            'base_table': 'table',
            'dependent_model': 'table'
        }

        # ensure that the model actually was materialized as a table
        for result in results:
            node_name = result.node.name
            self.assertEqual(result.node.config.materialized, expected_types[node_name])

    @use_profile('presto')
    def test__presto__changed_table_schema_for_downstream_view(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)

        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  2)
        self.assertManyTablesEqual(["people", "base_table", "dependent_model"])

        # Change the schema of base_table, assert that dependent_model doesn't fail
        results = self.run_dbt(["run", "--vars", "{add_table_field: true, dependent_type: view}"])
        self.assertEqual(len(results),  2)
        self.assertManyTablesEqual(["base_table", "dependent_model"])

    @use_profile('presto')
    def test__presto__changed_table_schema_for_downstream_view_changed_to_table(self):
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)

        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  2)
        self.assertManyTablesEqual(["people", "base_table", "dependent_model"])

        expected_types = {
            'base_table': 'table',
            'dependent_model': 'view'
        }

        # ensure that the model actually was materialized as a table
        for result in results:
            node_name = result.node.name
            self.assertEqual(result.node.config.materialized, expected_types[node_name])

        results = self.run_dbt(["run", "--vars", "{add_table_field: true, dependent_type: table}"])
        self.assertEqual(len(results),  2)
        self.assertManyTablesEqual(["base_table", "dependent_model"])

        expected_types = {
            'base_table': 'table',
            'dependent_model': 'table'
        }

        # ensure that the model actually was materialized as a table
        for result in results:
            node_name = result.node.name
            self.assertEqual(result.node.config.materialized, expected_types[node_name])
