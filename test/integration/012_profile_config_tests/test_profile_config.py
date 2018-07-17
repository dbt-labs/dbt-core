from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest
import dbt.deprecations

class TestNoRunTargetDeprecation(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("test/integration/012_profile_config_tests/seed.sql")
        dbt.deprecations.reset_deprecations()

    @property
    def schema(self):
        return "profile_config_012"

    @property
    def models(self):
        return "test/integration/012_profile_config_tests/models"

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
                        'schema': self.unique_schema()
                    }
                },
                'target': 'my-target'
            }
        }

    @attr(type='postgres')
    def test_deprecated_run_target_config(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed","view_model")

        self.assertFalse('run-target' in dbt.deprecations.active_deprecations)
