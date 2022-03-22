import pytest

from dbt.tests.util import run_dbt
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


def _verify_select_tag(results):
    assert len(results) == 1

    models_run = [r.node.name for r in results]
    assert "users" in models_run


def _verify_select_tag_and_children(results):
    assert len(results) == 3

    models_run = [r.node.name for r in results]
    assert "users" in models_run
    assert "users_rollup" in models_run


# check that model configs aren't squashed by project configs
def _verify_select_bi(results):
    assert len(results) == 2

    models_run = [r.node.name for r in results]
    assert "users" in models_run
    assert "users_rollup" in models_run


class TestTagSelection(SelectionFixtures):
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
        run_dbt(["seed"])
        results = run_dbt(["run", "--models", "tag:specified_as_string"])
        _verify_select_tag(results)

    def test_select_tag_selector_str(self, project):
        results = run_dbt(["run", "--selector", "tag_specified_as_string_str"])
        _verify_select_tag(results)

    def test_select_tag_selector_dict(self, project):
        results = run_dbt(["run", "--selector", "tag_specified_as_string_dict"])
        _verify_select_tag(results)

    def test_select_tag_and_children(self, project):  # noqa
        results = run_dbt(["run", "--models", "+tag:specified_in_project+"])
        _verify_select_tag_and_children(results)

    def test_select_tag_and_children_selector_str(self, project):  # noqa
        results = run_dbt(["run", "--selector", "tag_specified_in_project_children_str"])
        _verify_select_tag_and_children(results)

    def test_select_tag_and_children_selector_dict(self, project):  # noqa
        results = run_dbt(["run", "--selector", "tag_specified_in_project_children_dict"])
        _verify_select_tag_and_children(results)

    def test_select_tag_in_model_with_project_config(self, project):  # noqa
        results = run_dbt(["run", "--models", "tag:bi"])
        _verify_select_bi(results)

    def test_select_tag_in_model_with_project_config_selector(self, project):  # noqa
        results = run_dbt(["run", "--selector", "tagged-bi"])
        _verify_select_bi(results)

    # check that model configs aren't squashed by project configs
    def test_select_tag_in_model_with_project_config_parents_children(self, project):  # noqa
        results = run_dbt(["run", "--models", "@tag:users"])
        assert len(results) == 4

        models_run = set(r.node.name for r in results)
        assert {"users", "users_rollup", "emails_alt", "users_rollup_dependency"} == models_run

        # just the users/users_rollup tests
        results = run_dbt(["test", "--models", "@tag:users"])
        assert len(results) == 2
        assert sorted(r.node.name for r in results) == [
            "unique_users_id",
            "unique_users_rollup_gender",
        ]
        # just the email test
        results = run_dbt(["test", "--models", "tag:base,config.materialized:ephemeral"])
        assert len(results) == 1
        assert results[0].node.name == "not_null_emails_email"
        # also just the email test
        results = run_dbt(["test", "--models", "config.severity:warn"])
        assert len(results) == 1
        assert results[0].node.name == "not_null_emails_email"
        # all 3 tests
        results = run_dbt(
            ["test", "--models", "@tag:users tag:base,config.materialized:ephemeral"]
        )
        assert len(results) == 3
        assert sorted(r.node.name for r in results) == [
            "not_null_emails_email",
            "unique_users_id",
            "unique_users_rollup_gender",
        ]

    def test_select_tag_in_model_with_project_config_parents_children_selectors(self, project):
        results = run_dbt(["run", "--selector", "user_tagged_childrens_parents"])
        assert len(results) == 4

        models_run = set(r.node.name for r in results)
        assert {"users", "users_rollup", "emails_alt", "users_rollup_dependency"} == models_run

        # just the users/users_rollup tests
        results = run_dbt(["test", "--selector", "user_tagged_childrens_parents"])
        assert len(results) == 2
        assert sorted(r.node.name for r in results) == [
            "unique_users_id",
            "unique_users_rollup_gender",
        ]
        # just the email test
        results = run_dbt(["test", "--selector", "base_ephemerals"])
        assert len(results) == 1
        assert results[0].node.name == "not_null_emails_email"

        # also just the email test
        results = run_dbt(["test", "--selector", "warn-severity"])
        assert len(results) == 1
        assert results[0].node.name == "not_null_emails_email"
        # all 3 tests
        results = run_dbt(["test", "--selector", "roundabout-everything"])
        assert len(results) == 3
        assert sorted(r.node.name for r in results) == [
            "not_null_emails_email",
            "unique_users_id",
            "unique_users_rollup_gender",
        ]
