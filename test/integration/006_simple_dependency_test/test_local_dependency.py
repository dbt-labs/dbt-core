from test.integration.base import DBTIntegrationTest, use_profile
import os
import json
import shutil
from unittest import mock

import dbt.semver
import dbt.config
import dbt.exceptions


class BaseDependencyTest(DBTIntegrationTest):
    @property
    def schema(self):
        return "local_dependency_006"

    @property
    def models(self):
        return "local_models"

    def base_schema(self):
        return self.unique_schema()

    def configured_schema(self):
        return self.unique_schema() + '_configured'

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'local': 'local_dependency'
                }
            ]
        }


class TestSimpleDependency(BaseDependencyTest):

    @property
    def schema(self):
        return 'local_dependency_006'

    @property
    def models(self):
        return 'local_models'

    def base_schema(self):
        return self.unique_schema()

    def configured_schema(self):
        return self.unique_schema() + '_configured'

    @use_profile('postgres')
    def test_postgres_local_dependency(self):
        self.run_dbt(['deps'])
        self.run_dbt(['seed'])
        results = self.run_dbt(['run'])
        self.assertEqual(len(results),  3)
        self.assertEqual({r.node.schema for r in results},
                         {self.base_schema(), self.configured_schema()})
        self.assertEqual(
            len([r.node for r in results
                 if r.node.schema == self.base_schema()]),
            2
        )


class TestMissingDependency(DBTIntegrationTest):
    @property
    def schema(self):
        return "local_dependency_006"

    @property
    def models(self):
        return "sad_iteration_models"

    @use_profile('postgres')
    def test_postgres_missing_dependency(self):
        # dbt should raise a dbt exception, not raise a parse-time TypeError.
        with self.assertRaises(dbt.exceptions.Exception) as exc:
            self.run_dbt(['compile'])
        message = str(exc.exception)
        self.assertIn('no_such_dependency', message)
        self.assertIn('is undefined', message)


class TestSimpleDependencyWithSchema(TestSimpleDependency):
    @property
    def project_config(self):
        return {
            'macro-paths': ['schema_override_macros'],
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
        # check seed
        with self.assertRaises(dbt.exceptions.DbtProjectError) as exc:
            self.run_dbt(['seed'])
        self.assertIn('--no-version-check', str(exc.exception))
        # check run too
        with self.assertRaises(dbt.exceptions.DbtProjectError) as exc:
            self.run_dbt(['run'])
        self.assertIn('--no-version-check', str(exc.exception))

    @use_profile('postgres')
    @mock.patch('dbt.config.project.get_installed_version')
    def test_postgres_local_dependency_out_of_date_no_check(self, mock_get):
        mock_get.return_value = dbt.semver.VersionSpecifier.from_version_string('0.0.1')
        self.run_dbt(['deps'])
        self.run_dbt(['seed', '--no-version-check'])
        results = self.run_dbt(['run', '--no-version-check'])
        self.assertEqual(len(results), 3)


class TestSimpleDependencyHooks(DBTIntegrationTest):
    @property
    def schema(self):
        return "hooks_dependency_006"

    @property
    def models(self):
        return "hook_models"

    @property
    def project_config(self):
        # these hooks should run first, so nothing to drop
        return {
            'on-run-start': [
                "drop table if exists {{ var('test_create_table') }}",
                "drop table if exists {{ var('test_create_second_table') }}",
            ]
        }

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'local': 'early_hook_dependency'
                },
                {
                    'local': 'late_hook_dependency'
                }
            ]
        }

    def base_schema(self):
        return self.unique_schema()

    def configured_schema(self):
        return self.unique_schema() + '_configured'

    @use_profile('postgres')
    def test_postgres_hook_dependency(self):
        cli_vars = json.dumps({
            'test_create_table': '"{}"."hook_test"'.format(self.unique_schema()),
            'test_create_second_table': '"{}"."hook_test_2"'.format(self.unique_schema())
        })
        self.run_dbt(["deps", '--vars', cli_vars])
        results = self.run_dbt(["run", '--vars', cli_vars])
        self.assertEqual(len(results),  2)
        self.assertTablesEqual('actual', 'expected')


class TestSimpleDependencyDuplicateName(DBTIntegrationTest):
    @property
    def schema(self):
        return "local_dependency_006"

    @property
    def models(self):
        return "local_models"

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'local': 'duplicate_dependency'
                }
            ]
        }

    @use_profile('postgres')
    def test_postgres_local_dependency_same_name(self):
        with self.assertRaises(dbt.exceptions.DependencyException):
            self.run_dbt(['deps'], expect_pass=False)

    @use_profile('postgres')
    def test_postgres_local_dependency_same_name_sneaky(self):
        os.makedirs('dbt_modules')
        shutil.copytree('./duplicate_dependency', './dbt_modules/duplicate_dependency')
        with self.assertRaises(dbt.exceptions.CompilationException):
            self.run_dbt(['compile'], expect_pass=False)
