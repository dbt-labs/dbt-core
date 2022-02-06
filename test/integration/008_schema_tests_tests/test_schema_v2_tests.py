from test.integration.base import DBTIntegrationTest, FakeArgs, use_profile
import os

from dbt.task.test import TestTask
from dbt.exceptions import ParsingException
from dbt.contracts.results import TestStatus


class TestSchemaTests(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("seed.sql")
        self.run_sql_file("seed_failure.sql")

    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "models-v2/models"

    def run_schema_validations(self):
        args = FakeArgs()
        test_task = TestTask(args, self.config)
        return test_task.run()

    def assertTestFailed(self, result):
        self.assertEqual(result.status, "fail")
        self.assertFalse(result.skipped)
        self.assertTrue(
            result.failures > 0,
            'test {} did not fail'.format(result.node.name)
        )

    def assertTestPassed(self, result):
        self.assertEqual(result.status, "pass")
        self.assertFalse(result.skipped)
        self.assertEqual(
            result.failures, 0,
            'test {} failed'.format(result.node.name)
        )

    @use_profile('postgres')
    def test_postgres_schema_tests(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 5)
        test_results = self.run_schema_validations()
        # If the disabled model's tests ran, there would be 20 of these.
        self.assertEqual(len(test_results), 19)

        for result in test_results:
            # assert that all deliberately failing tests actually fail
            if 'failure' in result.node.name:
                self.assertTestFailed(result)
            # assert that actual tests pass
            else:
                self.assertTestPassed(result)
        self.assertEqual(sum(x.failures for x in test_results), 6)

    @use_profile('postgres')
    def test_postgres_schema_test_selection(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 5)
        test_results = self.run_dbt(
            ['test', '--models', 'tag:table_favorite_color'])
        # 1 in table_copy, 4 in table_summary
        self.assertEqual(len(test_results), 5)
        for result in test_results:
            self.assertTestPassed(result)

        test_results = self.run_dbt(
            ['test', '--models', 'tag:favorite_number_is_pi'])
        self.assertEqual(len(test_results), 1)
        self.assertTestPassed(test_results[0])

        test_results = self.run_dbt(
            ['test', '--models', 'tag:table_copy_favorite_color'])
        self.assertEqual(len(test_results), 1)
        self.assertTestPassed(test_results[0])

    @use_profile('postgres')
    def test_postgres_schema_test_exclude_failures(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 5)
        test_results = self.run_dbt(['test', '--exclude', 'tag:xfail'])
        # If the failed + disabled model's tests ran, there would be 20 of these.
        self.assertEqual(len(test_results), 13)
        for result in test_results:
            self.assertTestPassed(result)
        test_results = self.run_dbt(
            ['test', '--models', 'tag:xfail'], expect_pass=False)
        self.assertEqual(len(test_results), 6)
        for result in test_results:
            self.assertTestFailed(result)

class TestLimitedSchemaTests(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "models-v2/limit_null"

    def run_schema_validations(self):
        args = FakeArgs()
        test_task = TestTask(args, self.config)
        return test_task.run()
    
    def assertTestFailed(self, result):
        self.assertEqual(result.status, "fail")
        self.assertFalse(result.skipped)
        self.assertTrue(
            result.failures > 0,
            'test {} did not fail'.format(result.node.name)
        )

    def assertTestWarn(self, result):
        self.assertEqual(result.status, "warn")
        self.assertFalse(result.skipped)
        self.assertTrue(
            result.failures > 0,
            'test {} passed without expected warning'.format(result.node.name)
        )

    def assertTestPassed(self, result):
        self.assertEqual(result.status, "pass")
        self.assertFalse(result.skipped)
        self.assertEqual(
            result.failures, 0,
            'test {} failed'.format(result.node.name)
        )

    @use_profile('postgres')
    def test_postgres_limit_schema_tests(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 3)
        test_results = self.run_schema_validations()
        self.assertEqual(len(test_results), 3)

        for result in test_results:
            # assert that all deliberately failing tests actually fail
            if 'failure' in result.node.name:
                self.assertTestFailed(result)
            # assert that tests with warnings have them
            elif 'warning' in result.node.name:
                self.assertTestWarn(result)
            # assert that actual tests pass
            else:
                self.assertTestPassed(result)
        # warnings are also marked as failures
        self.assertEqual(sum(x.failures for x in test_results), 3)


class TestDefaultBoolType(DBTIntegrationTest):
    # test with default True/False in get_test_sql macro

    def setUp(self):
        DBTIntegrationTest.setUp(self)

    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "models-v2/override_get_test_models"

    def run_schema_validations(self):
        args = FakeArgs()
        test_task = TestTask(args, self.config)
        return test_task.run()

    def assertTestFailed(self, result):
        self.assertEqual(result.status, "fail")
        self.assertFalse(result.skipped)
        self.assertTrue(
            result.failures > 0,
            'test {} did not fail'.format(result.node.name)
        )

    def assertTestWarn(self, result):
        self.assertEqual(result.status, "warn")
        self.assertFalse(result.skipped)
        self.assertTrue(
            result.failures > 0,
            'test {} passed without expected warning'.format(result.node.name)
        )

    def assertTestPassed(self, result):
        self.assertEqual(result.status, "pass")
        self.assertFalse(result.skipped)
        self.assertEqual(
            result.failures, 0,
            'test {} failed'.format(result.node.name)
        )

    @use_profile('postgres')
    def test_postgres_limit_schema_tests(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 3)
        test_results = self.run_schema_validations()
        self.assertEqual(len(test_results), 3)

        for result in test_results:
            # assert that all deliberately failing tests actually fail
            if 'failure' in result.node.name:
                self.assertTestFailed(result)
            # assert that tests with warnings have them
            elif 'warning' in result.node.name:
                self.assertTestWarn(result)
            # assert that actual tests pass
            else:
                self.assertTestPassed(result)
        # warnings are also marked as failures
        self.assertEqual(sum(x.failures for x in test_results), 3)


class TestOtherBoolType(DBTIntegrationTest):
    # test with expected 0/1 in custom get_test_sql macro

    def setUp(self):
        DBTIntegrationTest.setUp(self)

    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "models-v2/override_get_test_models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ["macros-v2/override_get_test_macros"],
        }

    def run_schema_validations(self):
        args = FakeArgs()
        test_task = TestTask(args, self.config)
        return test_task.run()

    def assertTestFailed(self, result):
        self.assertEqual(result.status, "fail")
        self.assertFalse(result.skipped)
        self.assertTrue(
            result.failures > 0,
            'test {} did not fail'.format(result.node.name)
        )

    def assertTestWarn(self, result):
        self.assertEqual(result.status, "warn")
        self.assertFalse(result.skipped)
        self.assertTrue(
            result.failures > 0,
            'test {} passed without expected warning'.format(result.node.name)
        )

    def assertTestPassed(self, result):
        self.assertEqual(result.status, "pass")
        self.assertFalse(result.skipped)
        self.assertEqual(
            result.failures, 0,
            'test {} failed'.format(result.node.name)
        )

    @use_profile('postgres')
    def test_postgres_limit_schema_tests(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 3)
        test_results = self.run_schema_validations()
        self.assertEqual(len(test_results), 3)

        for result in test_results:
            # assert that all deliberately failing tests actually fail
            if 'failure' in result.node.name:
                self.assertTestFailed(result)
            # assert that tests with warnings have them
            elif 'warning' in result.node.name:
                self.assertTestWarn(result)
            # assert that actual tests pass
            else:
                self.assertTestPassed(result)
        # warnings are also marked as failures
        self.assertEqual(sum(x.failures for x in test_results), 3)


class TestNonBoolType(DBTIntegrationTest):
    # test with invalid 'x'/'y' in custom get_test_sql macro
    def setUp(self):
        DBTIntegrationTest.setUp(self)

    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "models-v2/override_get_test_models_fail"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ["macros-v2/override_get_test_macros_fail"],
        }

    def run_schema_validations(self):
        args = FakeArgs()

        test_task = TestTask(args, self.config)
        return test_task.run()

    @use_profile('postgres')
    def test_postgres_limit_schema_tests(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 1)
        run_result = self.run_dbt(['test'], expect_pass=False)
        results = run_result.results
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, TestStatus.Error)
        self.assertRegex(results[0].message, r"'get_test_sql' returns 'x'")


class TestMalformedSchemaTests(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "models-v2/malformed"

    def run_schema_validations(self):
        args = FakeArgs()

        test_task = TestTask(args, self.config)
        return test_task.run()

    @use_profile('postgres')
    def test_postgres_malformed_schema_will_break_run(self):
        with self.assertRaises(ParsingException):
            self.run_dbt()


class TestCustomConfigSchemaTests(DBTIntegrationTest):
    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return 'schema_tests_008'

    @property
    def models(self):
        return "models-v2/custom-configs"
    
    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ["macros-v2/custom-configs"],
        }

    @use_profile('postgres')
    def test_postgres_config(self):
        """ Test that tests use configs properly. All tests for
        this project will fail, configs are set to make test pass. """
        results = self.run_dbt(['test'], expect_pass=False)

        self.assertEqual(len(results), 8)
        for result in results:
            self.assertFalse(result.skipped)


class TestHooksInTests(DBTIntegrationTest):

    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        # this makes things easier
        return "ephemeral"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "on-run-start": ["{{ log('hooks called in tests -- good!') if execute }}"],
            "on-run-end": ["{{ log('hooks called in tests -- good!') if execute }}"],
        }

    @use_profile('postgres')
    def test_postgres_hooks_do_run_for_tests(self):
        # This passes now that hooks run, a behavior we changed in v1.0
        results = self.run_dbt(['test', '--model', 'ephemeral'])
        self.assertEqual(len(results), 1)
        for result in results:
            self.assertEqual(result.status, "pass")
            self.assertFalse(result.skipped)
            self.assertEqual(
                result.failures, 0,
                'test {} failed'.format(result.node.name)
            )

class TestHooksForWhich(DBTIntegrationTest):

    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        # test ephemeral models so we don't need to do a run (which would fail)
        return "ephemeral"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "on-run-start": ["{{exceptions.raise_compiler_error('hooks called in tests -- error') if (execute and flags.WHICH != 'test') }}"],
            "on-run-end": ["{{exceptions.raise_compiler_error('hooks called in tests -- error') if (execute and flags.WHICH != 'test') }}"],
        }

    @use_profile('postgres')
    def test_postgres_these_hooks_dont_run_for_tests(self):
        # This would fail if the hooks ran
        results = self.run_dbt(['test', '--model', 'ephemeral'])
        self.assertEqual(len(results), 1)
        for result in results:
            self.assertEqual(result.status, "pass")
            self.assertFalse(result.skipped)
            self.assertEqual(
                result.failures, 0,
                'test {} failed'.format(result.node.name)
            )


class TestCustomSchemaTests(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("seed.sql")

    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def packages_config(self):
        return {
            'packages': [
                {
                    "local": "./local_dependency",
                },
                {
                    'git': 'https://github.com/dbt-labs/dbt-integration-project',
                    'revision': 'dbt/1.0.0',
                },
            ]
        }

    @property
    def project_config(self):
        # dbt-utils contains a schema test (equality)
        # dbt-integration-project contains a schema.yml file
        # both should work!
        return {
            'config-version': 2,
            "macro-paths": ["macros-v2/macros"],
        }

    @property
    def models(self):
        return "models-v2/custom"

    def run_schema_validations(self):
        args = FakeArgs()

        test_task = TestTask(args, self.config)
        return test_task.run()

    @use_profile('postgres')
    def test_postgres_schema_tests(self):
        self.run_dbt(["deps"])
        results = self.run_dbt()
        self.assertEqual(len(results), 4)

        test_results = self.run_schema_validations()
        self.assertEqual(len(test_results), 6)

        expected_failures = [
            'not_null_table_copy_email',
            'every_value_is_blue_table_copy_favorite_color'
        ]

        for result in test_results:
            if result.status == 'fail':
                self.assertIn(result.node.name, expected_failures)


class TestQuotedSchemaTestColumns(DBTIntegrationTest):
    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "quote-required-models"

    @use_profile('postgres')
    def test_postgres_quote_required_column(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 3)
        results = self.run_dbt(['test', '-m', 'model'])
        self.assertEqual(len(results), 2)
        results = self.run_dbt(['test', '-m', 'model_again'])
        self.assertEqual(len(results), 2)
        results = self.run_dbt(['test', '-m', 'model_noquote'])
        self.assertEqual(len(results), 2)
        results = self.run_dbt(['test', '-m', 'source:my_source'])
        self.assertEqual(len(results), 1)
        results = self.run_dbt(['test', '-m', 'source:my_source_2'])
        self.assertEqual(len(results), 2)


class TestCliVarsSchemaTests(DBTIntegrationTest):
    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "models-v2/render_test_cli_arg_models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ["macros-v2/macros"],
        }

    @use_profile('postgres')
    def test_postgres_argument_rendering(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 1)
        results = self.run_dbt(['test', '--vars', '{myvar: foo}'])
        self.assertEqual(len(results), 1)
        self.run_dbt(['test'], expect_pass=False)


class TestConfiguredVarsSchemaTests(DBTIntegrationTest):
    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "models-v2/render_test_configured_arg_models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ["macros-v2/macros"],
            'vars': {
                'myvar': 'foo'
            }
        }

    @use_profile('postgres')
    def test_postgres_argument_rendering(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 1)
        results = self.run_dbt(['test'])
        self.assertEqual(len(results), 1)

class TestSchemaCaseInsensitive(DBTIntegrationTest):
    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "case-sensitive-models"

    @use_profile('postgres')
    def test_postgres_schema_lowercase_sql(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 2)
        results = self.run_dbt(['test', '-m', 'lowercase'])
        self.assertEqual(len(results), 1)

    @use_profile('postgres')
    def test_postgres_schema_uppercase_sql(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 2)
        results = self.run_dbt(['test', '-m', 'uppercase'])
        self.assertEqual(len(results), 1)


class TestSchemaTestContext(DBTIntegrationTest):
    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "test-context-models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ["test-context-macros"],
            "vars": {
                'local_utils_dispatch_list': ['local_utils']
            }
        }

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'local': 'local_utils'
                }
            ]
        }

    @use_profile('postgres')
    def test_postgres_test_context_tests(self):
        # This test tests the the TestContext and TestMacroNamespace
        # are working correctly
        self.run_dbt(['deps'])
        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        run_result = self.run_dbt(['test'], expect_pass=False)
        results = run_result.results
        results = sorted(results, key=lambda r: r.node.name)
        self.assertEqual(len(results), 5)
        # call_pkg_macro_model_c_
        self.assertEqual(results[0].status, TestStatus.Fail)
        # dispatch_model_c_
        self.assertEqual(results[1].status, TestStatus.Fail)
        # my_datediff
        self.assertRegex(results[2].node.compiled_sql, r'1000')
        # type_one_model_a_
        self.assertEqual(results[3].status, TestStatus.Fail)
        self.assertRegex(results[3].node.compiled_sql, r'union all')
        # type_two_model_a_
        self.assertEqual(results[4].status, TestStatus.Warn)
        self.assertEqual(results[4].node.config.severity, 'WARN')

class TestSchemaTestContextWithMacroNamespace(DBTIntegrationTest):
    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "test-context-models-namespaced"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ["test-context-macros-namespaced"],
            "dispatch": [
                {
                    "macro_namespace": "test_utils",
                    "search_order": ['local_utils', 'test_utils'],
                }
            ],
        }

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'local': 'test_utils'
                },
                {
                    'local': 'local_utils'
                },
            ]
        }

    @use_profile('postgres')
    def test_postgres_test_context_with_macro_namespace(self):
        # This test tests the the TestContext and TestMacroNamespace
        # are working correctly
        self.run_dbt(['deps'])
        results = self.run_dbt()
        self.assertEqual(len(results), 3)

        run_result = self.run_dbt(['test'], expect_pass=False)
        results = run_result.results
        results = sorted(results, key=lambda r: r.node.name)
        self.assertEqual(len(results), 4)
        # call_pkg_macro_model_c_
        self.assertEqual(results[0].status, TestStatus.Fail)
        # dispatch_model_c_
        self.assertEqual(results[1].status, TestStatus.Fail)
        # type_one_model_a_
        self.assertEqual(results[2].status, TestStatus.Fail)
        self.assertRegex(results[2].node.compiled_sql, r'union all')
        # type_two_model_a_
        self.assertEqual(results[3].status, TestStatus.Warn)
        self.assertEqual(results[3].node.config.severity, 'WARN')

class TestSchemaTestNameCollision(DBTIntegrationTest):
    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "name_collision"

    def run_schema_tests(self):
        args = FakeArgs()
        test_task = TestTask(args, self.config)
        return test_task.run()

    @use_profile('postgres')
    def test_postgres_collision_test_names_get_hash(self):
        """The models should produce unique IDs with a has appended"""
        results = self.run_dbt()
        test_results = self.run_schema_tests()

        # both models and both tests run
        self.assertEqual(len(results), 2)
        self.assertEqual(len(test_results), 2)

        # both tests have the same unique id except for the hash
        expected_unique_ids = [
            'test.test.not_null_base_extension_id.922d83a56c',
            'test.test.not_null_base_extension_id.c8d18fe069'
        ]
        self.assertIn(test_results[0].node.unique_id, expected_unique_ids)
        self.assertIn(test_results[1].node.unique_id, expected_unique_ids)


class TestInvalidSchema(DBTIntegrationTest):
    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "invalid-schema-models"

    @use_profile('postgres')
    def test_postgres_invalid_schema_file(self):
        with self.assertRaises(ParsingException) as exc:
            results = self.run_dbt()
        self.assertRegex(str(exc.exception), r"'models' is not a list")


class TestWrongSpecificationBlock(DBTIntegrationTest):
    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "wrong_specification_block"

    @use_profile('postgres')
    def test_postgres_wrong_specification_block(self):
        with self.assertWarns(Warning):
            results = self.run_dbt(['ls', '-s', 'some_seed', '--output', 'json', '--output-keys', 'name, description'])

        assert len(results) == 1
        assert results[0] == '{"name": "some_seed", "description": ""}'


class TestSchemaTestContextWhereSubq(DBTIntegrationTest):
    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "test-context-where-subq-models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ["test-context-where-subq-macros"],
        }

    @use_profile('postgres')
    def test_postgres_test_context_tests(self):
        # This test tests that get_where_subquery() is included in TestContext + TestMacroNamespace,
        # otherwise api.Relation.create() will return an error
        results = self.run_dbt()
        self.assertEqual(len(results), 1)

        results = self.run_dbt(['test'])
        self.assertEqual(len(results), 1)
