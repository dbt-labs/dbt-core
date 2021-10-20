from test.integration.base import DBTIntegrationTest, use_profile

import dbt.exceptions


class TestExitCodes(DBTIntegrationTest):

    @property
    def schema(self):
        return "exit_codes_test_023"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "snapshot-paths": ['snapshots-good'],
        }

    @use_profile('postgres')
    def test_postgres_exit_code_run_succeed(self):
        results, success = self.run_dbt_and_check(['run', '--model', 'good'])
        self.assertEqual(len(results.results), 1)
        self.assertTrue(success)
        self.assertTableDoesExist('good')

    @use_profile('postgres')
    def test__postgres_exit_code_run_fail(self):
        results, success = self.run_dbt_and_check(['run', '--model', 'bad'])
        self.assertEqual(len(results.results), 1)
        self.assertFalse(success)
        self.assertTableDoesNotExist('bad')

    @use_profile('postgres')
    def test__postgres_schema_test_pass(self):
        results, success = self.run_dbt_and_check(['run', '--model', 'good'])
        self.assertEqual(len(results.results), 1)
        self.assertTrue(success)
        results, success = self.run_dbt_and_check(['test', '--model', 'good'])
        self.assertEqual(len(results.results), 1)
        self.assertTrue(success)

    @use_profile('postgres')
    def test__postgres_schema_test_fail(self):
        results, success = self.run_dbt_and_check(['run', '--model', 'dupe'])
        self.assertEqual(len(results.results), 1)
        self.assertTrue(success)
        results, success = self.run_dbt_and_check(['test', '--model', 'dupe'])
        self.assertEqual(len(results.results), 1)
        self.assertFalse(success)

    @use_profile('postgres')
    def test__postgres_compile(self):
        results, success = self.run_dbt_and_check(['compile'])
        self.assertEqual(len(results.results), 7)
        self.assertTrue(success)

    @use_profile('postgres')
    def test__postgres_snapshot_pass(self):
        self.run_dbt_and_check(['run', '--model', 'good'])
        results, success = self.run_dbt_and_check(['snapshot'])
        self.assertEqual(len(results.results), 1)
        self.assertTableDoesExist('good_snapshot')
        self.assertTrue(success)


class TestExitCodesSnapshotFail(DBTIntegrationTest):

    @property
    def schema(self):
        return "exit_codes_test_023"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "snapshot-paths": ['snapshots-bad'],
        }

    @use_profile('postgres')
    def test__postgres_snapshot_fail(self):
        results, success = self.run_dbt_and_check(['run', '--model', 'good'])
        self.assertTrue(success)
        self.assertEqual(len(results.results), 1)

        results, success = self.run_dbt_and_check(['snapshot'])
        self.assertEqual(len(results.results), 1)
        self.assertTableDoesNotExist('good_snapshot')
        self.assertFalse(success)

class TestExitCodesDeps(DBTIntegrationTest):

    @property
    def schema(self):
        return "exit_codes_test_023"

    @property
    def models(self):
        return "models"

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'git': 'https://github.com/dbt-labs/dbt-integration-project',
                    'revision': 'dbt/1.0.0',
                }
            ]
        }

    @use_profile('postgres')
    def test_postgres_deps(self):
        _, success = self.run_dbt_and_check(['deps'])
        self.assertTrue(success)


class TestExitCodesDepsFail(DBTIntegrationTest):
    @property
    def schema(self):
        return "exit_codes_test_023"

    @property
    def models(self):
        return "models"

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'git': 'https://github.com/dbt-labs/dbt-integration-project',
                    'revision': 'bad-branch',
                },
            ]
        }

    @use_profile('postgres')
    def test_postgres_deps(self):
        with self.assertRaises(dbt.exceptions.InternalException):
            # this should fail
            self.run_dbt_and_check(['deps'])


class TestExitCodesSeed(DBTIntegrationTest):
    @property
    def schema(self):
        return "exit_codes_test_023"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds-good'],
            'seeds': {
                'quote_columns': False,
            },
        }

    @use_profile('postgres')
    def test_postgres_seed(self):
        results, success = self.run_dbt_and_check(['seed'])
        self.assertEqual(len(results.results), 1)
        self.assertTrue(success)


class TestExitCodesSeedFail(DBTIntegrationTest):
    @property
    def schema(self):
        return "exit_codes_test_023"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds-bad'],
            'seeds': {
                'quote_columns': False,
            },
        }

    @use_profile('postgres')
    def test_postgres_seed(self):
        _, success = self.run_dbt_and_check(['seed'])
        self.assertFalse(success)
