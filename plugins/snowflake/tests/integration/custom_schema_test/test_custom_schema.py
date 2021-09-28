from tests.integration.base import DBTIntegrationTest, use_profile

class TestCustomProjectSchemaWithPrefixSnowflake(DBTIntegrationTest):
    def setUp(self):
        super().setUp()
        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.v1_schema())
        )
        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.v2_schema())
        )
        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.xf_schema())
        )

    @property
    def schema(self):
        return "sf_custom_prefix"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "models": {
                "schema": "dbt_test"
            }
        }

    def v1_schema(self):
        return f"{self.unique_schema()}_DBT_TEST"

    def v2_schema(self):
        return f"{self.unique_schema()}_CUSTOM"

    def xf_schema(self):
        return f"{self.unique_schema()}_TEST"

    @use_profile('snowflake')
    def test__snowflake__custom_schema_with_prefix(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        schema = self.unique_schema().upper()

        self.assertTablesEqual("SEED", "VIEW_1", schema, self.v1_schema())
        self.assertTablesEqual("SEED", "VIEW_2", schema, self.v2_schema())
        self.assertTablesEqual("AGG", "VIEW_3", schema, self.xf_schema())
