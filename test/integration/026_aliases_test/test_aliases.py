from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest


class TestAliases(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

    @property
    def schema(self):
        return "aliases_026"

    @property
    def models(self):
        return "test/integration/026_aliases_test/models"

    @property
    def profile_config(self):
        return {
            'test': {
                'outputs': {
                    'dev': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': 'database',
                        'port': 5432,
                        'user': "root",
                        'pass': "password",
                        'dbname': 'dbt',
                        'schema': self.unique_schema()
                    },
                },
                'target': 'dev'
            }
        }

    @property
    def query_foo_alias(self):
        return """
            select
                tablename
            from {schema}.foo
        """.format(schema=self.unique_schema())

    @property
    def query_ref_foo_alias(self):
        return """
            select
                tablename
            from {schema}.ref_foo_alias
        """.format(schema=self.unique_schema())

    @attr(type='postgres')
    def test__alias_model_name(self):
        self.run_dbt(['run'])
        result = self.run_sql(self.query_foo_alias, fetch='all')[0][0]
        self.assertEqual(result, 'foo')
        result = self.run_sql(self.query_ref_foo_alias, fetch='all')[0][0]
        self.assertEqual(result, 'ref_foo_alias')
