from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest, FakeArgs

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

    @attr(type='postgres')
    def test_exit_code_run_succeed(self):
        self.use_default_project()
        self.use_profile('postgres')
        _, success = self.run_dbt_and_check(['run', '--model', 'good'])
        self.assertTrue(success)
        self.assertTableDoesExist('good')

    @attr(type='postgres')
    def test__exit_code_run_fail(self):
        self.use_default_project()
        self.use_profile('postgres')
        _, success = self.run_dbt_and_check(['run', '--model', 'bad'])
        self.assertFalse(success)
        self.assertTableDoesNotExist('bad')

    @attr(type='postgres')
    def test___schema_test_pass(self):
        self.use_default_project()
        self.use_profile('postgres')
        _, success = self.run_dbt_and_check(['run', '--model', 'good'])
        self.assertTrue(success)
        _, success = self.run_dbt_and_check(['test', '--model', 'good'])
        self.assertTrue(success)

    @attr(type='postgres')
    def test___schema_test_fail(self):
        self.use_default_project()
        self.use_profile('postgres')
        _, success = self.run_dbt_and_check(['run', '--model', 'dupe'])
        self.assertTrue(success)
        _, success = self.run_dbt_and_check(['test', '--model', 'dupe'])
        self.assertFalse(success)

    @attr(type='postgres')
    def test___compile(self):
        self.use_default_project()
        self.use_profile('postgres')
        _, success = self.run_dbt_and_check(['compile'])
        self.assertTrue(success)

    @attr(type='postgres')
    def test___archive_pass(self):
        self.use_default_project()
        self.use_profile('postgres')

        self.run_dbt_and_check(['run', '--model', 'good'])
        _, success = self.run_dbt_and_check(['archive'])
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

    @attr(type='postgres')
    def test___archive_fail(self):
        self.use_default_project()
        self.use_profile('postgres')

        _, success = self.run_dbt_and_check(['run', '--model', 'good'])
        self.assertTrue(success)

        _, success = self.run_dbt_and_check(['archive'])
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
    def project_config(self):
        return {
            "repositories": [
                'https://github.com/fishtown-analytics/dbt-integration-project'
            ]
        }

    @attr(type='postgres')
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
    def project_config(self):
        return {
            "repositories": [
                'https://github.com/fishtown-analytics/dbt-integration-project@bad-branch'
            ]
        }

    @attr(type='postgres')
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

    @attr(type='postgres')
    def test_seed(self):
        _, success = self.run_dbt_and_check(['seed'])
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

    @attr(type='postgres')
    def test_seed(self):
        try:
            _, success = self.run_dbt_and_check(['seed'])
            self.assertTrue(False)
        except dbt.exceptions.CompilationException as e:
            pass
