from test.integration.base import DBTIntegrationTest, FakeArgs, use_profile

import dbt.exceptions


class TestExitCodes(DBTIntegrationTest):

    @property
    def schema(self):
        return "exit_codes_test_023"

    @property
    def models(self):
        return "test/integration/023_exit_codes_test/models"

    @property
    def project_config(self):
        return {
            "archive": [
                {
                    "source_schema": self.unique_schema(),
                    "target_schema": self.unique_schema(),
                    "tables": [
                        {
                            "source_table": "good",
                            "target_table": "good_archive",
                            "updated_at": 'updated_at',
                            "unique_key": 'id'
                        }
                    ]
                }
            ]
        }

    @use_profile('postgres')
    def test_exit_code_run_succeed(self):
        results, success = self.run_dbt_and_check(['run', '--model', 'good'])
        self.assertEqual(len(results), 1)
        self.assertTrue(success)
        self.assertTableDoesExist('good')

    @use_profile('postgres')
    def test__exit_code_run_fail(self):
        results, success = self.run_dbt_and_check(['run', '--model', 'bad'])
        self.assertEqual(len(results), 1)
        self.assertFalse(success)
        self.assertTableDoesNotExist('bad')

    @use_profile('postgres')
    def test___schema_test_pass(self):
        results, success = self.run_dbt_and_check(['run', '--model', 'good'])
        self.assertEqual(len(results), 1)
        self.assertTrue(success)
        results, success = self.run_dbt_and_check(['test', '--model', 'good'])
        self.assertEqual(len(results), 1)
        self.assertTrue(success)

    @use_profile('postgres')
    def test___schema_test_fail(self):
        results, success = self.run_dbt_and_check(['run', '--model', 'dupe'])
        self.assertEqual(len(results), 1)
        self.assertTrue(success)
        results, success = self.run_dbt_and_check(['test', '--model', 'dupe'])
        self.assertEqual(len(results), 1)
        self.assertFalse(success)

    @use_profile('postgres')
    def test___compile(self):
        results, success = self.run_dbt_and_check(['compile'])
        self.assertEqual(len(results), 7)
        self.assertTrue(success)

    @use_profile('postgres')
    def test___archive_pass(self):
        self.run_dbt_and_check(['run', '--model', 'good'])
        results, success = self.run_dbt_and_check(['archive'])
        self.assertEqual(len(results), 1)
        self.assertTableDoesExist('good_archive')
        self.assertTrue(success)

class TestExitCodesArchiveFail(DBTIntegrationTest):

    @property
    def schema(self):
        return "exit_codes_test_023"

    @property
    def models(self):
        return "test/integration/023_exit_codes_test/models"

    @property
    def project_config(self):
        return {
            "archive": [
                {
                    "source_schema": self.unique_schema(),
                    "target_schema": self.unique_schema(),
                    "tables": [
                        {
                            "source_table": "good",
                            "target_table": "good_archive",
                            "updated_at": 'updated_at_not_real',
                            "unique_key": 'id'
                        }
                    ]
                }
            ]
        }

    @use_profile('postgres')
    def test___archive_fail(self):
        results, success = self.run_dbt_and_check(['run', '--model', 'good'])
        self.assertTrue(success)
        self.assertEqual(len(results), 1)

        results, success = self.run_dbt_and_check(['archive'])
        self.assertEqual(len(results), 1)
        self.assertTableDoesNotExist('good_archive')
        self.assertFalse(success)

class TestExitCodesDeps(DBTIntegrationTest):

    @property
    def schema(self):
        return "exit_codes_test_023"

    @property
    def models(self):
        return "test/integration/023_exit_codes_test/models"

    @property
    def packages_config(self):
        return {
            "packages": [
                {'git': 'https://github.com/fishtown-analytics/dbt-integration-project'}
            ]
        }

    @use_profile('postgres')
    def test_deps(self):
        _, success = self.run_dbt_and_check(['deps'])
        self.assertTrue(success)

class TestExitCodesDepsFail(DBTIntegrationTest):
    @property
    def schema(self):
        return "exit_codes_test_023"

    @property
    def models(self):
        return "test/integration/023_exit_codes_test/models"


    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'git': 'https://github.com/fishtown-analytics/dbt-integration-project',
                    'revision': 'bad-branch',
                },
            ]
        }

    @use_profile('postgres')
    def test_deps(self):
        # this should fail
        try:
            _, success = self.run_dbt_and_check(['deps'])
            self.assertTrue(False)
        except dbt.exceptions.InternalException as e:
            pass

class TestExitCodesSeed(DBTIntegrationTest):
    @property
    def schema(self):
        return "exit_codes_test_023"

    @property
    def models(self):
        return "test/integration/023_exit_codes_test/models"

    @property
    def project_config(self):
        return {
            "data-paths": ['test/integration/023_exit_codes_test/data-good']
        }

    @use_profile('postgres')
    def test_seed(self):
        results, success = self.run_dbt_and_check(['seed'])
        self.assertEqual(len(results), 1)
        self.assertTrue(success)

class TestExitCodesSeedFail(DBTIntegrationTest):
    @property
    def schema(self):
        return "exit_codes_test_023"

    @property
    def models(self):
        return "test/integration/023_exit_codes_test/models"

    @property
    def project_config(self):
        return {
            "data-paths": ['test/integration/023_exit_codes_test/data-bad']
        }

    @use_profile('postgres')
    def test_seed(self):
        try:
            _, success = self.run_dbt_and_check(['seed'])
            self.assertTrue(False)
        except dbt.exceptions.CompilationException as e:
            pass
