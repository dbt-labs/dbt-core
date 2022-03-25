import pytest
import os
import re

from dbt.tests.util import run_dbt, write_file
from tests.functional.schema_tests.fixtures import (  # noqa: F401
    wrong_specification_block,
    test_context_where_subq_models,
    test_utils,
    local_dependency,
    case_sensitive_models,
    test_context_macros,
    test_context_models_namespaced,
    macros_v2,
    test_context_macros_namespaced,
    seeds,
    test_context_models,
    name_collision,
    dupe_tests_collide,
    custom_generic_test_names,
    custom_generic_test_names_alt_format,
    test_context_where_subq_macros,
    invalid_schema_models,
    all_models,
    local_utils,
    ephemeral,
    quote_required_models,
    project_files,
    case_sensitive_models__uppercase_SQL,
)
from dbt.exceptions import ParsingException, CompilationException
from dbt.contracts.results import TestStatus


class TestSchemaTests:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql_file(os.path.join(project.test_data_dir, "seed.sql"))
        project.run_sql_file(os.path.join(project.test_data_dir, "seed_failure.sql"))

    @pytest.fixture(scope="class")
    def models(self, all_models):  # noqa: F811
        return all_models["models"]

    def assertTestFailed(self, result):
        assert result.status == "fail"
        assert not result.skipped
        assert result.failures > 0, "test {} did not fail".format(result.node.name)

    def assertTestPassed(self, result):
        assert result.status == "pass"
        assert not result.skipped
        assert result.failures == 0, "test {} failed".format(result.node.name)

    def test_schema_tests(
        self,
        project,
    ):
        results = run_dbt()
        assert len(results) == 5
        test_results = run_dbt(["test"])
        # If the disabled model's tests ran, there would be 20 of these.
        assert len(test_results) == 19

        for result in test_results:
            # assert that all deliberately failing tests actually fail
            if "failure" in result.node.name:
                self.assertTestFailed(result)
            # assert that actual tests pass
            else:
                self.assertTestPassed(result)
        assert sum(x.failures for x in test_results) == 6

    def test_schema_test_selection(
        self,
        project,
    ):
        results = run_dbt()
        assert len(results) == 5
        test_results = run_dbt(["test", "--models", "tag:table_favorite_color"])
        # 1 in table_copy, 4 in table_summary
        assert len(test_results) == 5
        for result in test_results:
            self.assertTestPassed(result)

        test_results = run_dbt(["test", "--models", "tag:favorite_number_is_pi"])
        assert len(test_results) == 1
        self.assertTestPassed(test_results[0])

        test_results = run_dbt(["test", "--models", "tag:table_copy_favorite_color"])
        assert len(test_results) == 1
        self.assertTestPassed(test_results[0])

    def test_schema_test_exclude_failures(
        self,
        project,
    ):
        results = run_dbt()
        assert len(results) == 5
        test_results = run_dbt(["test", "--exclude", "tag:xfail"])
        # If the failed + disabled model's tests ran, there would be 20 of these.
        assert len(test_results) == 13
        for result in test_results:
            self.assertTestPassed(result)
        test_results = run_dbt(["test", "--models", "tag:xfail"], expect_pass=False)
        assert len(test_results) == 6
        for result in test_results:
            self.assertTestFailed(result)


class TestLimitedSchemaTests:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql_file(os.path.join(project.test_data_dir, "seed.sql"))

    @pytest.fixture(scope="class")
    def models(self, all_models):  # noqa: F811
        return all_models["limit_null"]

    def assertTestFailed(self, result):
        assert result.status == "fail"
        assert not result.skipped
        assert result.failures > 0, "test {} did not fail".format(result.node.name)

    def assertTestWarn(self, result):
        assert result.status == "warn"
        assert not result.skipped
        assert result.failures > 0, "test {} passed without expected warning".format(
            result.node.name
        )

    def assertTestPassed(self, result):
        assert result.status == "pass"
        assert not result.skipped
        assert result.failures == 0, "test {} failed".format(result.node.name)

    def test_limit_schema_tests(
        self,
        project,
    ):
        results = run_dbt()
        assert len(results) == 3
        test_results = run_dbt(["test"])
        assert len(test_results) == 3

        for result in test_results:
            # assert that all deliberately failing tests actually fail
            if "failure" in result.node.name:
                self.assertTestFailed(result)
            # assert that tests with warnings have them
            elif "warning" in result.node.name:
                self.assertTestWarn(result)
            # assert that actual tests pass
            else:
                self.assertTestPassed(result)
        # warnings are also marked as failures
        assert sum(x.failures for x in test_results) == 3


class TestDefaultBoolType:
    # test with default True/False in get_test_sql macro
    @pytest.fixture(scope="class")
    def models(self, all_models):  # noqa: F811
        return all_models["override_get_test_models"]

    def assertTestFailed(self, result):
        assert result.status == "fail"
        assert not result.skipped
        assert result.failures > 0, "test {} did not fail".format(result.node.name)

    def assertTestWarn(self, result):
        assert result.status == "warn"
        assert not result.skipped
        assert result.failures > 0, "test {} passed without expected warning".format(
            result.node.name
        )

    def assertTestPassed(self, result):
        assert result.status == "pass"
        assert not result.skipped
        assert result.failures == 0, "test {} failed".format(result.node.name)

    def test_limit_schema_tests(
        self,
        project,
    ):
        results = run_dbt()
        assert len(results) == 3
        test_results = run_dbt(["test"])
        assert len(test_results) == 3

        for result in test_results:
            # assert that all deliberately failing tests actually fail
            if "failure" in result.node.name:
                self.assertTestFailed(result)
            # assert that tests with warnings have them
            elif "warning" in result.node.name:
                self.assertTestWarn(result)
            # assert that actual tests pass
            else:
                self.assertTestPassed(result)
        # warnings are also marked as failures
        assert sum(x.failures for x in test_results) == 3


class TestOtherBoolType:
    # test with expected 0/1 in custom get_test_sql macro
    @pytest.fixture(scope="class")
    def models(self, all_models):  # noqa: F811
        return all_models["override_get_test_models"]

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["macros-v2/override_get_test_macros"],
        }

    def assertTestFailed(self, result):
        assert result.status == "fail"
        assert not result.skipped
        assert result.failures > 0, "test {} did not fail".format(result.node.name)

    def assertTestWarn(self, result):
        assert result.status == "warn"
        assert not result.skipped
        assert result.failures > 0, "test {} passed without expected warning".format(
            result.node.name
        )

    def assertTestPassed(self, result):
        assert result.status == "pass"
        assert not result.skipped
        assert result.failures == 0, "test {} failed".format(result.node.name)

    def test_limit_schema_tests(
        self,
        project,
    ):
        results = run_dbt()
        assert len(results) == 3
        test_results = run_dbt(["test"])
        assert len(test_results) == 3

        for result in test_results:
            # assert that all deliberately failing tests actually fail
            if "failure" in result.node.name:
                self.assertTestFailed(result)
            # assert that tests with warnings have them
            elif "warning" in result.node.name:
                self.assertTestWarn(result)
            # assert that actual tests pass
            else:
                self.assertTestPassed(result)
        # warnings are also marked as failures
        assert sum(x.failures for x in test_results) == 3


class TestNonBoolType:
    # test with invalid 'x'/'y' in custom get_test_sql macro
    @pytest.fixture(scope="class")
    def models(self, all_models):  # noqa: F811
        return all_models["override_get_test_models_fail"]

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["macros-v2/override_get_test_macros_fail"],
        }

    def test_limit_schema_tests(
        self,
        project,
    ):
        results = run_dbt()
        assert len(results) == 1
        run_result = run_dbt(["test"], expect_pass=False)
        results = run_result.results
        assert len(results) == 1
        assert results[0].status == TestStatus.Error
        assert re.search(r"'get_test_sql' returns 'x'", results[0].message)


class TestMalformedSchemaTests:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql_file(os.path.join(project.test_data_dir, "seed.sql"))

    @pytest.fixture(scope="class")
    def models(self, all_models):  # noqa: F811
        return all_models["malformed"]

    def test_malformed_schema_will_break_run(
        self,
        project,
    ):
        with pytest.raises(ParsingException):
            run_dbt()


class TestCustomConfigSchemaTests:
    @pytest.fixture(scope="class")
    def models(self, all_models):  # noqa: F811
        return all_models["custom-configs"]

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql_file(os.path.join(project.test_data_dir, "seed.sql"))

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["macros-v2/custom-configs"],
        }

    def test_config(
        self,
        project,
    ):
        """Test that tests use configs properly. All tests for
        this project will fail, configs are set to make test pass."""
        results = run_dbt(["test"], expect_pass=False)

        assert len(results) == 8
        for result in results:
            assert not result.skipped


class TestHooksInTests:
    @pytest.fixture(scope="class")
    def models(self, ephemeral):  # noqa: F811
        return ephemeral

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "on-run-start": ["{{ log('hooks called in tests -- good!') if execute }}"],
            "on-run-end": ["{{ log('hooks called in tests -- good!') if execute }}"],
        }

    def test_hooks_do_run_for_tests(
        self,
        project,
    ):
        # This passes now that hooks run, a behavior we changed in v1.0
        results = run_dbt(["test", "--model", "ephemeral"])
        assert len(results) == 1
        for result in results:
            assert result.status == "pass"
            assert not result.skipped
            assert result.failures == 0, "test {} failed".format(result.node.name)


class TestHooksForWhich:
    @pytest.fixture(scope="class")
    def models(self, ephemeral):  # noqa: F811
        return ephemeral

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "on-run-start": [
                "{{exceptions.raise_compiler_error('hooks called in tests -- error') if (execute and flags.WHICH != 'test') }}"
            ],
            "on-run-end": [
                "{{exceptions.raise_compiler_error('hooks called in tests -- error') if (execute and flags.WHICH != 'test') }}"
            ],
        }

    def test_these_hooks_dont_run_for_tests(
        self,
        project,
    ):
        # This would fail if the hooks ran
        results = run_dbt(["test", "--model", "ephemeral"])
        assert len(results) == 1
        for result in results:
            assert result.status == "pass"
            assert not result.skipped
            assert result.failures == 0, "test {} failed".format(result.node.name)


class TestCustomSchemaTests:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql_file(os.path.join(project.test_data_dir, "seed.sql"))

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "local": "./local_dependency",
                },
                {
                    "git": "https://github.com/dbt-labs/dbt-integration-project",
                    "revision": "dbt/1.0.0",
                },
            ]
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # dbt-utils contains a schema test (equality)
        # dbt-integration-project contains a schema.yml file
        # both should work!
        return {
            "config-version": 2,
            "macro-paths": ["macros-v2/macros"],
        }

    @pytest.fixture(scope="class")
    def models(self, all_models):  # noqa: F811
        return all_models["custom"]

    def test_schema_tests(
        self,
        project,
    ):
        run_dbt(["deps"])
        results = run_dbt()
        assert len(results) == 4

        test_results = run_dbt(["test"])
        assert len(test_results) == 6

        expected_failures = [
            "not_null_table_copy_email",
            "every_value_is_blue_table_copy_favorite_color",
        ]

        for result in test_results:
            if result.status == "fail":
                assert result.node.name in expected_failures


class TestQuotedSchemaTestColumns:
    @pytest.fixture(scope="class")
    def models(self, quote_required_models):  # noqa: F811
        return quote_required_models

    def test_quote_required_column(
        self,
        project,
    ):
        results = run_dbt()
        assert len(results) == 3
        results = run_dbt(["test", "-m", "model"])
        assert len(results) == 2
        results = run_dbt(["test", "-m", "model_again"])
        assert len(results) == 2
        results = run_dbt(["test", "-m", "model_noquote"])
        assert len(results) == 2
        results = run_dbt(["test", "-m", "source:my_source"])
        assert len(results) == 1
        results = run_dbt(["test", "-m", "source:my_source_2"])
        assert len(results) == 2


class TestCliVarsSchemaTests:
    @pytest.fixture(scope="class")
    def models(self, all_models):  # noqa: F811
        return all_models["render_test_cli_arg_models"]

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["macros-v2/macros"],
        }

    def test_argument_rendering(
        self,
        project,
    ):
        results = run_dbt()
        assert len(results) == 1
        results = run_dbt(["test", "--vars", "{myvar: foo}"])
        assert len(results) == 1
        run_dbt(["test"], expect_pass=False)


class TestConfiguredVarsSchemaTests:
    @pytest.fixture(scope="class")
    def models(self, all_models):  # noqa: F811
        return all_models["render_test_configured_arg_models"]

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["macros-v2/macros"],
            "vars": {"myvar": "foo"},
        }

    def test_argument_rendering(
        self,
        project,
    ):
        results = run_dbt()
        assert len(results) == 1
        results = run_dbt(["test"])
        assert len(results) == 1


class TestSchemaCaseInsensitive:
    @pytest.fixture(scope="class")
    def models(self, case_sensitive_models):  # noqa: F811
        return case_sensitive_models

    @pytest.fixture(scope="class", autouse=True)
    def setUP(self, project):
        # Create the uppercase SQL file
        model_dir = os.path.join(project.project_root, "models")
        write_file(case_sensitive_models__uppercase_SQL, model_dir, "uppercase.SQL")

    def test_schema_lowercase_sql(
        self,
        project,
    ):
        results = run_dbt()
        assert len(results) == 2
        results = run_dbt(["test", "-m", "lowercase"])
        assert len(results) == 1

    def test_schema_uppercase_sql(
        self,
        project,
    ):
        results = run_dbt()
        assert len(results) == 2
        results = run_dbt(["test", "-m", "uppercase"])
        assert len(results) == 1


class TestSchemaTestContext:
    @pytest.fixture(scope="class")
    def models(self, test_context_models):  # noqa: F811
        return test_context_models

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["test-context-macros"],
            "vars": {"local_utils_dispatch_list": ["local_utils"]},
        }

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "local_utils"}]}

    def test_test_context_tests(
        self,
        project,
    ):
        # This test tests the the TestContext and TestMacroNamespace
        # are working correctly
        run_dbt(["deps"])
        results = run_dbt()
        assert len(results) == 3

        run_result = run_dbt(["test"], expect_pass=False)
        results = run_result.results
        results = sorted(results, key=lambda r: r.node.name)
        assert len(results) == 5
        # call_pkg_macro_model_c_
        assert results[0].status == TestStatus.Fail
        # dispatch_model_c_
        assert results[1].status == TestStatus.Fail
        # my_datediff
        assert re.search(r"1000", results[2].node.compiled_sql)
        # type_one_model_a_
        assert results[3].status == TestStatus.Fail
        assert re.search(r"union all", results[3].node.compiled_sql)
        # type_two_model_a_
        assert results[4].status == TestStatus.Warn
        assert results[4].node.config.severity == "WARN"


class TestSchemaTestContextWithMacroNamespace:
    @pytest.fixture(scope="class")
    def models(self, test_context_models_namespaced):  # noqa: F811
        return test_context_models_namespaced

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["test-context-macros-namespaced"],
            "dispatch": [
                {
                    "macro_namespace": "test_utils",
                    "search_order": ["local_utils", "test_utils"],
                }
            ],
        }

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"local": "test_utils"},
                {"local": "local_utils"},
            ]
        }

    def test_test_context_with_macro_namespace(
        self,
        project,
    ):
        # This test tests the the TestContext and TestMacroNamespace
        # are working correctly
        run_dbt(["deps"])
        results = run_dbt()
        assert len(results) == 3

        run_result = run_dbt(["test"], expect_pass=False)
        results = run_result.results
        results = sorted(results, key=lambda r: r.node.name)
        assert len(results) == 4
        # call_pkg_macro_model_c_
        assert results[0].status == TestStatus.Fail
        # dispatch_model_c_
        assert results[1].status == TestStatus.Fail
        # type_one_model_a_
        assert results[2].status == TestStatus.Fail
        assert re.search(r"union all", results[2].node.compiled_sql)
        # type_two_model_a_
        assert results[3].status == TestStatus.Warn
        assert results[3].node.config.severity == "WARN"


class TestSchemaTestNameCollision:
    @pytest.fixture(scope="class")
    def models(self, name_collision):  # noqa: F811
        return name_collision

    def test_collision_test_names_get_hash(
        self,
        project,
    ):
        """The models should produce unique IDs with a has appended"""
        results = run_dbt()
        test_results = run_dbt(["test"])

        # both models and both tests run
        assert len(results) == 2
        assert len(test_results) == 2

        # both tests have the same unique id except for the hash
        expected_unique_ids = [
            "test.test.not_null_base_extension_id.922d83a56c",
            "test.test.not_null_base_extension_id.c8d18fe069",
        ]
        assert test_results[0].node.unique_id in expected_unique_ids
        assert test_results[1].node.unique_id in expected_unique_ids


class TestGenericTestsCollide:
    @pytest.fixture(scope="class")
    def models(self, dupe_tests_collide):  # noqa: F811
        return dupe_tests_collide

    def test_generic_test_collision(
        self,
        project,
    ):
        """These tests collide, since only the configs differ"""
        with pytest.raises(CompilationException) as exc:
            run_dbt()
        assert "dbt found two tests with the name" in str(exc)


class TestGenericTestsCustomNames:
    @pytest.fixture(scope="class")
    def models(self, custom_generic_test_names):  # noqa: F811
        return custom_generic_test_names

    # users can define custom names for specific instances of generic tests
    def test_generic_tests_with_custom_names(
        self,
        project,
    ):
        """These tests don't collide, since they have user-provided custom names"""
        results = run_dbt()
        test_results = run_dbt(["test"])

        # model + both tests run
        assert len(results) == 1
        assert len(test_results) == 2

        # custom names propagate to the unique_id
        expected_unique_ids = [
            "test.test.not_null_where_1_equals_1.7b96089006",
            "test.test.not_null_where_1_equals_2.8ae586e17f",
        ]
        assert test_results[0].node.unique_id in expected_unique_ids
        assert test_results[1].node.unique_id in expected_unique_ids


class TestGenericTestsCustomNamesAltFormat(TestGenericTestsCustomNames):
    @pytest.fixture(scope="class")
    def models(self, custom_generic_test_names_alt_format):  # noqa: F811
        return custom_generic_test_names_alt_format

    # exactly as above, just alternative format for yaml definition
    def test_collision_test_names_get_hash(
        self,
        project,
    ):
        """These tests don't collide, since they have user-provided custom names,
        defined using an alternative format"""
        super().test_generic_tests_with_custom_names(project)


class TestInvalidSchema:
    @pytest.fixture(scope="class")
    def models(self, invalid_schema_models):  # noqa: F811
        return invalid_schema_models

    def test_invalid_schema_file(
        self,
        project,
    ):
        with pytest.raises(ParsingException) as exc:
            run_dbt()
        assert re.search(r"'models' is not a list", str(exc))


class TestWrongSpecificationBlock:
    @pytest.fixture(scope="class")
    def models(self, wrong_specification_block):  # noqa: F811
        return wrong_specification_block

    def test_wrong_specification_block(
        self,
        project,
    ):
        with pytest.warns(Warning):
            results = run_dbt(
                [
                    "ls",
                    "-s",
                    "some_seed",
                    "--output",
                    "json",
                    "--output-keys",
                    "name, description",
                ]
            )

        assert len(results) == 1
        assert results[0] == '{"name": "some_seed", "description": ""}'


class TestSchemaTestContextWhereSubq:
    @pytest.fixture(scope="class")
    def models(self, test_context_where_subq_models):  # noqa: F811
        return test_context_where_subq_models

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["test-context-where-subq-macros"],
        }

    def test_test_context_tests(
        self,
        project,
    ):
        # This test tests that get_where_subquery() is included in TestContext + TestMacroNamespace,
        # otherwise api.Relation.create() will return an error
        results = run_dbt()
        assert len(results) == 1

        results = run_dbt(["test"])
        assert len(results) == 1
