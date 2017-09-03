from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest


class TestCustomSchema(DBTIntegrationTest):

    @property
    def schema(self):
        return "custom_schema_024"

    @property
    def models(self):
        return "test/integration/024_custom_schema_test/models"

    @attr(type='postgres')
    def test__postgres__custom_schema_no_prefix(self):
        self.use_default_project()
        self.run_sql_file("test/integration/024_custom_schema_test/seed.sql")

        self.run_dbt()

        schema = self.unique_schema()
        v2_schema = "{}_custom".format(schema)
        xf_schema = "{}_test".format(schema)

        self.assertTablesEqual("seed","view_1")
        self.assertTablesEqual("seed","view_2", schema, v2_schema)
        self.assertTablesEqual("agg","view_3", schema, xf_schema)


class TestCustomSchemaWithPrefix(DBTIntegrationTest):

    @property
    def schema(self):
        return "custom_schema_024"

    @property
    def models(self):
        return "test/integration/024_custom_schema_test/models"

    @property
    def profile_config(self, schema=None, schema_prefix=None):
        return {
            'test': {
                'outputs': {
                    'my-target': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': 'database',
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.unique_schema(),
                        # do this to avoid conflicts between tests
                        'schema_prefix': "analytics_{}_".format(self.unique_schema()),
                    }
                },
                'target': 'my-target'
            }
        }

    @attr(type='postgres')
    def test__postgres__custom_schema_with_prefix(self):
        self.use_default_project()
        self.run_sql_file("test/integration/024_custom_schema_test/seed.sql")

        self.run_dbt()

        schema = self.unique_schema()
        v2_schema = "analytics_{}_custom".format(schema)
        xf_schema = "analytics_{}_test".format(schema)

        self.assertTablesEqual("seed","view_1", schema, schema)
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
                        'host': 'database',
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.unique_schema(),
                        # do this to avoid conflicts between tests
                        'schema_prefix': "analytics_{}_".format(self.unique_schema()),
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

    @attr(type='postgres')
    def test__postgres__custom_schema_with_prefix(self):
        self.use_default_project()
        self.run_sql_file("test/integration/024_custom_schema_test/seed.sql")

        self.run_dbt()

        schema = self.unique_schema()
        v1_schema = "analytics_{}_dbt_test".format(schema)
        v2_schema = "analytics_{}_custom".format(schema)
        xf_schema = "analytics_{}_test".format(schema)

        self.assertTablesEqual("seed","view_1", schema, v1_schema)
        self.assertTablesEqual("seed","view_2", schema, v2_schema)
        self.assertTablesEqual("agg","view_3", schema, xf_schema)
