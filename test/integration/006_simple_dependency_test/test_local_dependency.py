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

    def expected_schema(self):
        return self.unique_schema()

    @attr(type='postgres')
    def test_postgres_local_dependency(self):
        self.run_dbt(["deps"])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  2)
        self.assertTrue(all(r.node.schema == self.expected_schema()
                            for r in results))



class TestSimpleDependencyWithSchema(TestSimpleDependency):
    @property
    def project_config(self):
        return {
            'macro-paths': ['test/integration/006_simple_dependency_test/schema_override_macros'],
            'models': {
                'schema': 'dbt_test',
            }
        }

    def expected_schema(self):
        return 'dbt_test_{}_macro'.format(self.unique_schema())
