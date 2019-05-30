from test.integration.base import DBTIntegrationTest, use_profile


class TestCustomSchema(DBTIntegrationTest):

    @property
    def schema(self):
        return "custom_schema_024"

    @property
    def models(self):
        return "test/integration/024_custom_schema_test/models"

    @use_profile('postgres')
    def test__postgres__custom_schema_no_prefix(self):
        self.use_default_project()
        self.run_sql_file("test/integration/024_custom_schema_test/seed.sql")

        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        schema = self.unique_schema()
        v2_schema = "{}_custom".format(schema)
        xf_schema = "{}_test".format(schema)

        self.assertTablesEqual("seed","view_1")
        self.assertTablesEqual("seed","view_2", schema, v2_schema)
        self.assertTablesEqual("agg","view_3", schema, xf_schema)


class TestCustomProjectSchemaWithPrefix(DBTIntegrationTest):

    @property
    def schema(self):
        return "custom_schema_024"

    @property
    def models(self):
        return "test/integration/024_custom_schema_test/models"

    @property
    def profile_config(self):
        return {
            'test': {
                'outputs': {
                    'my-target': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': self.database_host,
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.unique_schema(),
                    }
                },
                'target': 'my-target'
            }
        }

    @property
    def project_config(self):
        return {
            "models": {
                "schema": "dbt_test"
            }
        }

    @use_profile('postgres')
    def test__postgres__custom_schema_with_prefix(self):
        self.use_default_project()
        self.run_sql_file("test/integration/024_custom_schema_test/seed.sql")

        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        schema = self.unique_schema()
        v1_schema = "{}_dbt_test".format(schema)
        v2_schema = "{}_custom".format(schema)
        xf_schema = "{}_test".format(schema)

        self.assertTablesEqual("seed","view_1", schema, v1_schema)
        self.assertTablesEqual("seed","view_2", schema, v2_schema)
        self.assertTablesEqual("agg","view_3", schema, xf_schema)


class TestCustomProjectSchemaWithPrefixSnowflake(DBTIntegrationTest):

    @property
    def schema(self):
        return "custom_schema_024"

    @property
    def models(self):
        return "test/integration/024_custom_schema_test/models"

    @property
    def project_config(self):
        return {
            "models": {
                "schema": "dbt_test"
            }
        }

    @use_profile('snowflake')
    def test__snowflake__custom_schema_with_prefix(self):
        self.use_default_project()
        self.run_sql_file("test/integration/024_custom_schema_test/seed.sql")

        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        schema = self.unique_schema().upper()
        v1_schema = "{}_DBT_TEST".format(schema)
        v2_schema = "{}_CUSTOM".format(schema)
        xf_schema = "{}_TEST".format(schema)

        self.assertTablesEqual("SEED","VIEW_1", schema, v1_schema)
        self.assertTablesEqual("SEED","VIEW_2", schema, v2_schema)
        self.assertTablesEqual("AGG","VIEW_3", schema, xf_schema)


class TestCustomSchemaWithCustomMacro(DBTIntegrationTest):

    @property
    def schema(self):
        return "custom_schema_024"

    @property
    def models(self):
        return "test/integration/024_custom_schema_test/models"

    @property
    def profile_config(self):
        return {
            'test': {
                'outputs': {
                    'prod': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': self.database_host,
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.unique_schema(),
                    }
                },
                'target': 'prod'
            }
        }

    @property
    def project_config(self):
        return {
            'macro-paths': ['test/integration/024_custom_schema_test/macros'],
            'models': {
                'schema': 'dbt_test'
            }
        }

    @use_profile('postgres')
    def test__postgres__custom_schema_from_macro(self):
        self.use_default_project()
        self.run_sql_file("test/integration/024_custom_schema_test/seed.sql")

        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        schema = self.unique_schema()
        v1_schema = "dbt_test_{}_macro".format(schema)
        v2_schema = "custom_{}_macro".format(schema)
        xf_schema = "test_{}_macro".format(schema)

        self.assertTablesEqual("seed","view_1", schema, v1_schema)
        self.assertTablesEqual("seed","view_2", schema, v2_schema)
        self.assertTablesEqual("agg","view_3", schema, xf_schema)
