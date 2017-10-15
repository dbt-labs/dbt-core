from nose.plugins.attrib import attr

from dbt.exceptions import CompilationException
from test.integration.base import DBTIntegrationTest


class TestDuplicateModel(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

    @property
    def schema(self):
        return "duplicate_model_025"

    @property
    def models(self):
        return "test/integration/025_duplicate_model_test/models/"

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

    @attr(type='postgres')
    def test_duplicate_model(self):
        message = 'Found models with the same name:.*'
        with self.assertRaisesRegexp(CompilationException, message):
            self.run_dbt(['run'])
