from test.integration.base import DBTIntegrationTest, use_profile
import mock

import dbt.semver
import dbt.config
import dbt.exceptions

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

    @use_profile('postgres')
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

    @use_profile('postgres')
    @mock.patch('dbt.config.project.get_installed_version')
    def test_postgres_local_dependency_out_of_date(self, mock_get):
        mock_get.return_value = dbt.semver.VersionSpecifier.from_version_string('0.0.1')
        self.run_dbt(['deps'])
        with self.assertRaises(dbt.exceptions.DbtProjectError) as exc:
            self.run_dbt(['run'])
        self.assertIn('--no-version-check', str(exc.exception))

    @use_profile('postgres')
    @mock.patch('dbt.config.project.get_installed_version')
    def test_postgres_local_dependency_out_of_date_no_check(self, mock_get):
        mock_get.return_value = dbt.semver.VersionSpecifier.from_version_string('0.0.1')
        self.run_dbt(['deps'])
        results = self.run_dbt(['run', '--no-version-check'])
        self.assertEqual(len(results), 3)
