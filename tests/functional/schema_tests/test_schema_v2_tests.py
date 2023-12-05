import pytest
import os
import re

from dbt.tests.util import run_dbt, write_file
from dbt.tests.fixtures.project import write_project_files
from tests.fixtures.dbt_integration_project import dbt_integration_project  # noqa: F401
from tests.functional.schema_tests.fixtures import (
    wrong_specification_block__schema_yml,
    test_context_where_subq_models__schema_yml,
    test_context_where_subq_models__model_a_sql,
    test_utils__dbt_project_yml,
    test_utils__macros__current_timestamp_sql,
    test_utils__macros__custom_test_sql,
    local_dependency__dbt_project_yml,
    local_dependency__macros__equality_sql,
    case_sensitive_models__schema_yml,
    case_sensitive_models__lowercase_sql,
    test_context_macros__my_test_sql,
    test_context_macros__test_my_datediff_sql,
    test_context_macros__custom_schema_tests_sql,
    test_context_models_namespaced__schema_yml,
    test_context_models_namespaced__model_c_sql,
    test_context_models_namespaced__model_b_sql,
    test_context_models_namespaced__model_a_sql,
    macros_v2__override_get_test_macros_fail__get_test_sql_sql,
    macros_v2__macros__tests_sql,
    macros_v2__custom_configs__test_sql,
    macros_v2__override_get_test_macros__get_test_sql_sql,
    test_context_macros_namespaced__my_test_sql,
    test_context_macros_namespaced__custom_schema_tests_sql,
    seeds__some_seed_csv,
    test_context_models__schema_yml,
    test_context_models__model_c_sql,
    test_context_models__model_b_sql,
    test_context_models__model_a_sql,
    name_collision__schema_yml,
    name_collision__base_sql,
    name_collision__base_extension_sql,
    dupe_generic_tests_collide__schema_yml,
    dupe_generic_tests_collide__model_a,
    custom_generic_test_config_custom_macro__schema_yml,
    custom_generic_test_config_custom_macro__model_a,
    custom_generic_test_names__schema_yml,
    custom_generic_test_names__model_a,
    custom_generic_test_names_alt_format__schema_yml,
    custom_generic_test_names_alt_format__model_a,
    test_context_where_subq_macros__custom_generic_test_sql,
    invalid_schema_models__schema_yml,
    invalid_schema_models__model_sql,
    models_v2__models__schema_yml,
    models_v2__models__table_summary_sql,
    models_v2__models__table_failure_summary_sql,
    models_v2__models__table_disabled_sql,
    models_v2__models__table_failure_null_relation_sql,
    models_v2__models__table_failure_copy_sql,
    models_v2__models__table_copy_sql,
    models_v2__limit_null__schema_yml,
    models_v2__limit_null__table_warning_limit_null_sql,
    models_v2__limit_null__table_limit_null_sql,
    models_v2__limit_null__table_failure_limit_null_sql,
    models_v2__override_get_test_models__schema_yml,
    models_v2__override_get_test_models__my_model_warning_sql,
    models_v2__override_get_test_models__my_model_pass_sql,
    models_v2__override_get_test_models__my_model_failure_sql,
    models_v2__override_get_test_models_fail__schema_yml,
    models_v2__override_get_test_models_fail__my_model_sql,
    models_v2__malformed__schema_yml,
    models_v2__malformed__table_summary_sql,
    models_v2__malformed__table_copy_sql,
    models_v2__custom_configs__schema_yml,
    models_v2__custom_configs__table_copy_another_one_sql,
    models_v2__custom_configs__table_copy_sql,
    models_v2__custom_configs__table_copy_with_dots_sql,
    models_v2__custom__schema_yml,
    models_v2__custom__table_copy_sql,
    models_v2__render_test_cli_arg_models__schema_yml,
    models_v2__render_test_cli_arg_models__model_sql,
    models_v2__render_test_configured_arg_models__schema_yml,
    models_v2__render_test_configured_arg_models__model_sql,
    local_utils__dbt_project_yml,
    local_utils__macros__datediff_sql,
    local_utils__macros__current_timestamp_sql,
    local_utils__macros__custom_test_sql,
    ephemeral__schema_yml,
    ephemeral__ephemeral_sql,
    quote_required_models__schema_yml,
    quote_required_models__model_again_sql,
    quote_required_models__model_noquote_sql,
    quote_required_models__model_sql,
    case_sensitive_models__uppercase_SQL,
    macro_resolution_order_macros__my_custom_test_sql,
    macro_resolution_order_models__config_yml,
    macro_resolution_order_models__my_model_sql,
    alt_local_utils__macros__type_timestamp_sql,
    all_quotes_schema__schema_yml,
)
from dbt.exceptions import ParsingError, CompilationError, DuplicateResourceNameError
from dbt.contracts.results import TestStatus


class TestSchemaTests:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql_file(os.path.join(project.test_data_dir, "seed.sql"))
        project.run_sql_file(os.path.join(project.test_data_dir, "seed_failure.sql"))

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_v2__models__schema_yml,
            "table_summary.sql": models_v2__models__table_summary_sql,
            "table_failure_summary.sql": models_v2__models__table_failure_summary_sql,
            "table_disabled.sql": models_v2__models__table_disabled_sql,
            "table_failure_null_relation.sql": models_v2__models__table_failure_null_relation_sql,
            "table_failure_copy.sql": models_v2__models__table_failure_copy_sql,
            "table_copy.sql": models_v2__models__table_copy_sql,
        }

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
        test_results = run_dbt(["test"], expect_pass=False)
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
    def models(self):
        return {
            "schema.yml": models_v2__limit_null__schema_yml,
            "table_warning_limit_null.sql": models_v2__limit_null__table_warning_limit_null_sql,
            "table_limit_null.sql": models_v2__limit_null__table_limit_null_sql,
            "table_failure_limit_null.sql": models_v2__limit_null__table_failure_limit_null_sql,
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
        test_results = run_dbt(["test"], expect_pass=False)
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
    def models(self):
        return {
            "schema.yml": models_v2__override_get_test_models__schema_yml,
            "my_model_warning.sql": models_v2__override_get_test_models__my_model_warning_sql,
            "my_model_pass.sql": models_v2__override_get_test_models__my_model_pass_sql,
            "my_model_failure.sql": models_v2__override_get_test_models__my_model_failure_sql,
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
        test_results = run_dbt(["test"], expect_pass=False)
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
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root):
        macros_v2_file = {
            "override_get_test_macros": {
                "get_test_sql.sql": macros_v2__override_get_test_macros__get_test_sql_sql
            },
        }
        write_project_files(project_root, "macros-v2", macros_v2_file)

    # test with expected 0/1 in custom get_test_sql macro
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_v2__override_get_test_models__schema_yml,
            "my_model_warning.sql": models_v2__override_get_test_models__my_model_warning_sql,
            "my_model_pass.sql": models_v2__override_get_test_models__my_model_pass_sql,
            "my_model_failure.sql": models_v2__override_get_test_models__my_model_failure_sql,
        }

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
        test_results = run_dbt(["test"], expect_pass=False)
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
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root):
        macros_v2_file = {
            "override_get_test_macros_fail": {
                "get_test_sql.sql": macros_v2__override_get_test_macros_fail__get_test_sql_sql
            },
        }
        write_project_files(project_root, "macros-v2", macros_v2_file)

    # test with invalid 'x'/'y' in custom get_test_sql macro
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_v2__override_get_test_models_fail__schema_yml,
            "my_model.sql": models_v2__override_get_test_models_fail__my_model_sql,
        }

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
    def models(self):
        return {
            "schema.yml": models_v2__malformed__schema_yml,
            "table_summary.sql": models_v2__malformed__table_summary_sql,
            "table_copy.sql": models_v2__malformed__table_copy_sql,
        }

    def test_malformed_schema_will_break_run(
        self,
        project,
    ):
        with pytest.raises(ParsingError):
            run_dbt()


class TestCustomConfigSchemaTests:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project, project_root):
        project.run_sql_file(os.path.join(project.test_data_dir, "seed.sql"))

        macros_v2_file = {"custom-configs": {"test.sql": macros_v2__custom_configs__test_sql}}
        write_project_files(project_root, "macros-v2", macros_v2_file)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_v2__custom_configs__schema_yml,
            "table_copy_another_one.sql": models_v2__custom_configs__table_copy_another_one_sql,
            "table_copy.sql": models_v2__custom_configs__table_copy_sql,
            "table.copy.with.dots.sql": models_v2__custom_configs__table_copy_with_dots_sql,
        }

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
    def models(self):
        return {
            "schema.yml": ephemeral__schema_yml,
            "ephemeral.sql": ephemeral__ephemeral_sql,
        }

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
    def models(self):
        return {
            "schema.yml": ephemeral__schema_yml,
            "ephemeral.sql": ephemeral__ephemeral_sql,
        }

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
    def setUp(self, project, project_root, dbt_integration_project):  # noqa: F811
        write_project_files(project_root, "dbt_integration_project", dbt_integration_project)
        project.run_sql_file(os.path.join(project.test_data_dir, "seed.sql"))

        local_dependency_files = {
            "dbt_project.yml": local_dependency__dbt_project_yml,
            "macros": {"equality.sql": local_dependency__macros__equality_sql},
        }
        write_project_files(project_root, "local_dependency", local_dependency_files)

        macros_v2_file = {
            "macros": {"tests.sql": macros_v2__macros__tests_sql},
        }
        write_project_files(project_root, "macros-v2", macros_v2_file)

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "local": "./local_dependency",
                },
                {
                    "local": "./dbt_integration_project",
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
    def models(self):
        return {
            "schema.yml": models_v2__custom__schema_yml,
            "table_copy.sql": models_v2__custom__table_copy_sql,
        }

    def test_schema_tests(
        self,
        project,
    ):
        run_dbt(["deps"])
        results = run_dbt()
        assert len(results) == 4

        test_results = run_dbt(["test"], expect_pass=False)
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
    def models(self):
        return {
            "schema.yml": quote_required_models__schema_yml,
            "model_again.sql": quote_required_models__model_again_sql,
            "model_noquote.sql": quote_required_models__model_noquote_sql,
            "model.sql": quote_required_models__model_sql,
        }

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
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root):
        macros_v2_file = {
            "macros": {"tests.sql": macros_v2__macros__tests_sql},
        }
        write_project_files(project_root, "macros-v2", macros_v2_file)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_v2__render_test_cli_arg_models__schema_yml,
            "model.sql": models_v2__render_test_cli_arg_models__model_sql,
        }

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
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root):
        macros_v2_file = {
            "macros": {"tests.sql": macros_v2__macros__tests_sql},
        }
        write_project_files(project_root, "macros-v2", macros_v2_file)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models_v2__render_test_configured_arg_models__schema_yml,
            "model.sql": models_v2__render_test_configured_arg_models__model_sql,
        }

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
    def models(self):
        return {
            "schema.yml": case_sensitive_models__schema_yml,
            "lowercase.sql": case_sensitive_models__lowercase_sql,
        }

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
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root):
        local_utils_files = {
            "dbt_project.yml": local_utils__dbt_project_yml,
            "macros": {
                "datediff.sql": local_utils__macros__datediff_sql,
                "current_timestamp.sql": local_utils__macros__current_timestamp_sql,
                "custom_test.sql": local_utils__macros__custom_test_sql,
            },
        }
        write_project_files(project_root, "local_utils", local_utils_files)

        test_context_macros_files = {
            "my_test.sql": test_context_macros__my_test_sql,
            "test_my_datediff.sql": test_context_macros__test_my_datediff_sql,
            "custom_schema_tests.sql": test_context_macros__custom_schema_tests_sql,
        }
        write_project_files(project_root, "test-context-macros", test_context_macros_files)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": test_context_models__schema_yml,
            "model_c.sql": test_context_models__model_c_sql,
            "model_b.sql": test_context_models__model_b_sql,
            "model_a.sql": test_context_models__model_a_sql,
        }

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

    def test_test_context_tests(self, project):
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
        assert re.search(r"1000", results[2].node.compiled_code)
        # type_one_model_a_
        assert results[3].status == TestStatus.Fail
        assert re.search(r"union all", results[3].node.compiled_code)
        # type_two_model_a_
        assert results[4].status == TestStatus.Warn
        assert results[4].node.config.severity == "WARN"


class TestSchemaTestContextWithMacroNamespace:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root):
        test_utils_files = {
            "dbt_project.yml": test_utils__dbt_project_yml,
            "macros": {
                "current_timestamp.sql": test_utils__macros__current_timestamp_sql,
                "custom_test.sql": test_utils__macros__custom_test_sql,
            },
        }
        write_project_files(project_root, "test_utils", test_utils_files)

        local_utils_files = {
            "dbt_project.yml": local_utils__dbt_project_yml,
            "macros": {
                "datediff.sql": local_utils__macros__datediff_sql,
                "current_timestamp.sql": local_utils__macros__current_timestamp_sql,
                "custom_test.sql": local_utils__macros__custom_test_sql,
            },
        }
        write_project_files(project_root, "local_utils", local_utils_files)

        test_context_macros_namespaced_file = {
            "my_test.sql": test_context_macros_namespaced__my_test_sql,
            "custom_schema_tests.sql": test_context_macros_namespaced__custom_schema_tests_sql,
        }
        write_project_files(
            project_root, "test-context-macros-namespaced", test_context_macros_namespaced_file
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": test_context_models_namespaced__schema_yml,
            "model_c.sql": test_context_models_namespaced__model_c_sql,
            "model_b.sql": test_context_models_namespaced__model_b_sql,
            "model_a.sql": test_context_models_namespaced__model_a_sql,
        }

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
        assert re.search(r"union all", results[2].node.compiled_code)
        # type_two_model_a_
        assert results[3].status == TestStatus.Warn
        assert results[3].node.config.severity == "WARN"


class TestSchemaTestNameCollision:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": name_collision__schema_yml,
            "base.sql": name_collision__base_sql,
            "base_extension.sql": name_collision__base_extension_sql,
        }

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
    def models(self):
        return {
            "schema.yml": dupe_generic_tests_collide__schema_yml,
            "model_a.sql": dupe_generic_tests_collide__model_a,
        }

    def test_generic_test_collision(
        self,
        project,
    ):
        """These tests collide, since only the configs differ"""
        with pytest.raises(DuplicateResourceNameError) as exc:
            run_dbt()
        assert "dbt found two tests with the name" in str(exc.value)


class TestGenericTestsConfigCustomMacros:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": custom_generic_test_config_custom_macro__schema_yml,
            "model_a.sql": custom_generic_test_config_custom_macro__model_a,
        }

    def test_generic_test_config_custom_macros(
        self,
        project,
    ):
        """This test has a reference to a custom macro its configs"""
        with pytest.raises(CompilationError) as exc:
            run_dbt()
        assert "Invalid generic test configuration" in str(exc)


class TestGenericTestsCustomNames:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": custom_generic_test_names__schema_yml,
            "model_a.sql": custom_generic_test_names__model_a,
        }

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
    def models(self):
        return {
            "schema.yml": custom_generic_test_names_alt_format__schema_yml,
            "model_a.sql": custom_generic_test_names_alt_format__model_a,
        }

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
    def models(self):
        return {
            "schema.yml": invalid_schema_models__schema_yml,
            "model.sql": invalid_schema_models__model_sql,
        }

    def test_invalid_schema_file(
        self,
        project,
    ):
        with pytest.raises(ParsingError) as exc:
            run_dbt()
        assert re.search(r"'models' is not a list", str(exc))


class TestCommentedSchema:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": all_quotes_schema__schema_yml,
            "model.sql": invalid_schema_models__model_sql,
        }

    def test_quoted_schema_file(self, project):
        try:
            # A schema file consisting entirely of quotes should not be a problem
            run_dbt(["parse"])
        except TypeError:
            assert (
                False
            ), "`dbt parse` failed with a yaml file that is all comments with the same exception as 3568"
        except Exception:
            assert False, "`dbt parse` failed with a yaml file that is all comments"


class TestWrongSpecificationBlock:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": wrong_specification_block__schema_yml}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"some_seed.csv": seeds__some_seed_csv}

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
                    "name",
                    "description",
                ]
            )

        assert len(results) == 1
        assert results[0] == '{"name": "some_seed", "description": ""}'


class TestSchemaTestContextWhereSubq:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root):
        test_context_where_subq_macros_file = {
            "custom_generic_test.sql": test_context_where_subq_macros__custom_generic_test_sql
        }
        write_project_files(
            project_root, "test-context-where-subq-macros", test_context_where_subq_macros_file
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": test_context_where_subq_models__schema_yml,
            "model_a.sql": test_context_where_subq_models__model_a_sql,
        }

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


class TestCustomSchemaTestMacroResolutionOrder:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root):
        alt_local_utils_file = {
            "dbt_project.yml": local_utils__dbt_project_yml,
            "macros": {
                "datediff.sql": alt_local_utils__macros__type_timestamp_sql,
            },
        }
        write_project_files(project_root, "alt_local_utils", alt_local_utils_file)

        macros_resolution_order_file = {
            "my_custom_test.sql": macro_resolution_order_macros__my_custom_test_sql,
        }
        write_project_files(
            project_root, "macro_resolution_order_macros", macros_resolution_order_file
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": macro_resolution_order_models__config_yml,
            "my_model.sql": macro_resolution_order_models__my_model_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["macro_resolution_order_macros"],
        }

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "alt_local_utils"}]}

    def test_macro_resolution_test_namespace(
        self,
        project,
    ):
        # https://github.com/dbt-labs/dbt-core/issues/5720
        # Previously, macros called as 'dbt.some_macro' would not correctly
        # resolve to 'some_macro' from the 'dbt' namespace during static analysis,
        # if 'some_macro' also existed in an installed package,
        # leading to the macro being missing in the TestNamespace
        run_dbt(["deps"])
        run_dbt(["parse"])
