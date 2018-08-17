from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest

class TestSnowflakeLateBindingViewDependency(DBTIntegrationTest):

    @property
    def schema(self):
        return "snowflake_view_dependency_test_036"

    @property
    def models(self):
        return "test/integration/036_snowflake_view_dependency_test/models"

    @property
    def project_config(self):
        return {
            "data-paths": ["test/integration/036_snowflake_view_dependency_test/data"],
            "quoting": {
                "schema": False,
                "identifier": False
            }
        }

    @attr(type='snowflake')
    def test__snowflake__weirdness(self):
        self.use_profile('snowflake')
        results = self.run_dbt(["seed"])
        self.assertEqual(len(results),  1)

        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  2)
        self.assertManyTablesEqual(["PEOPLE", "BASE_TABLE", "DEPENDENT_VIEW"])

        results = self.run_dbt(["run", "--vars", "add_table_field: true"])
        self.assertEqual(len(results),  2)
        self.assertManyTablesEqual(["BASE_TABLE", "DEPENDENT_VIEW"])
