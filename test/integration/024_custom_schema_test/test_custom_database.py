from test.integration.base import DBTIntegrationTest, use_profile


class TestOverrideDatabase(DBTIntegrationTest):
    setup_alternate_db = True

    @property
    def schema(self):
        return "custom_schema_024"

    @property
    def models(self):
        return "db-models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['custom-db-macros'],
        }

    @use_profile('snowflake')
    def test_snowflake_override_generate_db_name(self):
        self.run_sql_file('seed.sql')
        self.assertTableDoesExist('SEED', schema=self.unique_schema(), database=self.default_database)
        self.assertTableDoesExist('AGG', schema=self.unique_schema(), database=self.default_database)

        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        self.assertTableDoesExist('VIEW_1', schema=self.unique_schema(), database=self.default_database)
        self.assertTableDoesExist('VIEW_2', schema=self.unique_schema(), database=self.alternative_database)
        self.assertTableDoesExist('VIEW_3', schema=self.unique_schema(), database=self.alternative_database)

        # not overridden
        self.assertTablesEqual('SEED', 'VIEW_1', table_b_db=self.default_database)
        # overridden
        self.assertTablesEqual('SEED', 'VIEW_2', table_b_db=self.alternative_database)
        self.assertTablesEqual('AGG', 'VIEW_3', table_b_db=self.alternative_database)
