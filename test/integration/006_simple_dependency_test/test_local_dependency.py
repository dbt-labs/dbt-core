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

    @attr(type='postgres')
    def test_local_dependency(self):
        self.run_dbt(["deps"])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results),  2)

