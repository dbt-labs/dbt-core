import pytest

from dbt.tests.util import run_dbt, check_result_nodes_by_name
from tests.functional.graph_selection.fixtures import SelectionFixtures


selectors_yml = """
            selectors:
              - name: tag_specified_as_string_str
                definition: tag:specified_as_string
              - name: tag_specified_as_string_dict
                definition:
                  method: tag
                  value: specified_as_string
              - name: tag_specified_in_project_children_str
                definition: +tag:specified_in_project+
              - name: tag_specified_in_project_children_dict
                definition:
                  method: tag
                  value: specified_in_project
                  parents: true
                  children: true
              - name: tagged-bi
                definition:
                  method: tag
                  value: bi
              - name: user_tagged_childrens_parents
                definition:
                  method: tag
                  value: users
                  childrens_parents: true
              - name: base_ephemerals
                definition:
                  union:
                    - tag: base
                    - method: config.materialized
                      value: ephemeral
              - name: warn-severity
                definition:
                    config.severity: warn
              - name: roundabout-everything
                definition:
                    union:
                        - "@tag:users"
                        - intersection:
                            - tag: base
                            - config.materialized: ephemeral
    """


class TestTagSelection(SelectionFixtures):
    # The tests here aiming to test whether the correct node is selected,
    # we don't need the run to pass
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "models": {
                "test": {
                    "users": {"tags": "specified_as_string"},
                    "users_rollup": {
                        "tags": ["specified_in_project"],
                    },
                }
            },
        }

    @pytest.fixture(scope="class")
    def selectors(self):
        return selectors_yml

    def test_select_tag(self, project):
        results = run_dbt(["run", "--models", "tag:specified_as_string"], expect_pass=False)
        check_result_nodes_by_name(results, ["users"])

    def test_select_tag_selector_str(self, project):
        results = run_dbt(["run", "--selector", "tag_specified_as_string_str"], expect_pass=False)
        check_result_nodes_by_name(results, ["users"])

    def test_select_tag_selector_dict(self, project):
        results = run_dbt(["run", "--selector", "tag_specified_as_string_dict"], expect_pass=False)
        check_result_nodes_by_name(results, ["users"])

    def test_select_tag_and_children(self, project):  # noqa
        results = run_dbt(["run", "--models", "+tag:specified_in_project+"], expect_pass=False)
        check_result_nodes_by_name(results, ["users", "users_rollup", "users_rollup_dependency"])

    def test_select_tag_and_children_selector_str(self, project):  # noqa
        results = run_dbt(
            ["run", "--selector", "tag_specified_in_project_children_str"],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["users", "users_rollup", "users_rollup_dependency"])

    def test_select_tag_and_children_selector_dict(self, project):  # noqa
        results = run_dbt(
            ["run", "--selector", "tag_specified_in_project_children_dict"],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["users", "users_rollup", "users_rollup_dependency"])

    def test_select_tag_in_model_with_project_config(self, project):  # noqa
        results = run_dbt(["run", "--models", "tag:bi"], expect_pass=False)
        check_result_nodes_by_name(results, ["users", "users_rollup"])

    def test_select_tag_in_model_with_project_config_selector(self, project):  # noqa
        results = run_dbt(["run", "--selector", "tagged-bi"], expect_pass=False)
        check_result_nodes_by_name(results, ["users", "users_rollup"])

    # check that model configs aren't squashed by project configs
    def test_select_tag_in_model_with_project_config_parents_children(self, project):  # noqa
        results = run_dbt(["run", "--models", "@tag:users"], expect_pass=False)
        check_result_nodes_by_name(
            results, ["users", "users_rollup", "emails_alt", "users_rollup_dependency"]
        )

        # just the users/users_rollup tests
        results = run_dbt(["test", "--models", "@tag:users"], expect_pass=False)
        check_result_nodes_by_name(results, ["unique_users_rollup_gender", "unique_users_id"])

        # just the email test
        results = run_dbt(
            ["test", "--models", "tag:base,config.materialized:ephemeral"],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["not_null_emails_email"])

        # also just the email test
        results = run_dbt(["test", "--models", "config.severity:warn"], expect_pass=False)
        check_result_nodes_by_name(results, ["not_null_emails_email"])

        # all 3 tests
        results = run_dbt(
            ["test", "--models", "@tag:users tag:base,config.materialized:ephemeral"],
            expect_pass=False,
        )
        check_result_nodes_by_name(
            results,
            ["not_null_emails_email", "unique_users_id", "unique_users_rollup_gender"],
        )

    def test_select_tag_in_model_with_project_config_parents_children_selectors(self, project):
        results = run_dbt(
            ["run", "--selector", "user_tagged_childrens_parents"], expect_pass=False
        )
        check_result_nodes_by_name(
            results, ["users", "users_rollup", "emails_alt", "users_rollup_dependency"]
        )

        # just the users/users_rollup tests
        results = run_dbt(
            ["test", "--selector", "user_tagged_childrens_parents"], expect_pass=False
        )
        check_result_nodes_by_name(results, ["unique_users_id", "unique_users_rollup_gender"])

        # just the email test
        results = run_dbt(["test", "--selector", "base_ephemerals"], expect_pass=False)
        check_result_nodes_by_name(results, ["not_null_emails_email"])

        # also just the email test
        results = run_dbt(["test", "--selector", "warn-severity"], expect_pass=False)
        check_result_nodes_by_name(results, ["not_null_emails_email"])

        # all 3 tests
        results = run_dbt(["test", "--selector", "roundabout-everything"], expect_pass=False)
        check_result_nodes_by_name(
            results,
            ["unique_users_rollup_gender", "unique_users_id", "not_null_emails_email"],
        )
