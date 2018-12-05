from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest

class TestSimpleDependency(DBTIntegrationTest):

    @property
    def schema(self):
        return "local_dependency_006"

    @property
    def models(self):
        return "test/integration/006_simple_dependency_test/local_models"

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'local': 'test/integration/006_simple_dependency_test/local_dependency'
                }
            ]
        }

    def base_schema(self):
        return self.unique_schema()

    def configured_schema(self):
        return self.unique_schema() + '_configured'

    @attr(type='postgres')
    def test_postgres_local_dependency(self):
        self.run_dbt(["deps"])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  3)
        self.assertEqual({r.node.schema for r in results},
                         {self.base_schema(), self.configured_schema()})
        self.assertEqual(
            len([r.node for r in results
                 if r.node.schema == self.base_schema()]),
            2
        )



class TestSimpleDependencyWithSchema(TestSimpleDependency):
    @property
    def project_config(self):
        return {
            'macro-paths': ['test/integration/006_simple_dependency_test/schema_override_macros'],
            'models': {
                'schema': 'dbt_test',
            }
        }

    def base_schema(self):
        return 'dbt_test_{}_macro'.format(self.unique_schema())

    def configured_schema(self):
        return 'configured_{}_macro'.format(self.unique_schema())
