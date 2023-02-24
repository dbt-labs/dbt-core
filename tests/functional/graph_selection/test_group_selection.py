import pytest

from dbt.tests.util import run_dbt, read_file
from tests.functional.graph_selection.fixtures import (
    schema_yml,
    base_users_sql,
    users_sql,
    users_rollup_sql,
    users_rollup_dependency_sql,
    emails_sql,
    emails_alt_sql,
    alternative_users_sql,
    never_selected_sql,
    subdir_sql,
    nested_users_sql,
    properties_yml,
)


selectors_yml = """
            selectors:
              - name: group_specified_as_string_str
                definition: group:users_group
              - name: group_specified_as_string_dict
                definition:
                  method: group
                  value: users_group
              - name: users_grouped_childrens_parents
                definition:
                  method: group
                  value: users_group
                  childrens_parents: true
"""


class TestGroupSelection:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "base_users.sql": base_users_sql,
            "users.sql": users_sql,
            "users_rollup.sql": users_rollup_sql,
            "users_rollup_dependency.sql": users_rollup_dependency_sql,
            "emails.sql": emails_sql,
            "emails_alt.sql": emails_alt_sql,
            "alternative.users.sql": alternative_users_sql,
            "never_selected.sql": never_selected_sql,
            "test": {
                "subdir.sql": subdir_sql,
                "subdir": {"nested_users.sql": nested_users_sql},
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self, test_data_dir):
        # Read seed file and return
        seeds = {"properties.yml": properties_yml}
        for seed_file in ["seed.csv", "summary_expected.csv"]:
            seeds[seed_file] = read_file(test_data_dir, seed_file)
        return seeds

    @pytest.fixture(scope="class")
    def selectors(self):
        return selectors_yml

    def test_select_models_by_group(self, project):
        results = run_dbt(["ls", "--model", "group:users_group"])
        assert sorted(results) == ["test.users"]

    def test_select_group_selector_str(self, project):
        results = run_dbt(["ls", "--selector", "group_specified_as_string_str"])
        assert sorted(results) == ["test.unique_users_id", "test.users"]

    def test_select_group_selector_dict(self, project):
        results = run_dbt(["ls", "--selector", "group_specified_as_string_dict"])
        assert sorted(results) == ["test.unique_users_id", "test.users"]

    def test_select_models_by_group_and_children(self, project):  # noqa
        results = run_dbt(["ls", "--models", "+group:users_group+"])
        assert sorted(results) == [
            "test.base_users",
            "test.emails_alt",
            "test.users",
            "test.users_rollup",
            "test.users_rollup_dependency",
        ]

    def test_select_group_and_children(self, project):  # noqa
        expected = [
            "exposure:test.user_exposure",
            "source:test.raw.seed",
            "test.base_users",
            "test.emails_alt",
            "test.unique_users_id",
            "test.unique_users_rollup_gender",
            "test.users",
            "test.users_rollup",
            "test.users_rollup_dependency",
        ]
        results = run_dbt(["ls", "--select", "+group:users_group+"])
        assert sorted(results) == expected

    def test_select_group_and_children_selector_str(self, project):  # noqa
        expected = [
            "exposure:test.user_exposure",
            "source:test.raw.seed",
            "test.base_users",
            "test.emails_alt",
            "test.unique_users_id",
            "test.unique_users_rollup_gender",
            "test.users",
            "test.users_rollup",
            "test.users_rollup_dependency",
        ]
        results = run_dbt(["ls", "--selector", "users_grouped_childrens_parents"])
        assert sorted(results) == expected

    # 2 groups
    def test_select_models_two_groups(self, project):
        expected = ["test.base_users", "test.emails", "test.users"]
        results = run_dbt(["ls", "--models", "@group:emails_group group:users_group"])
        assert sorted(results) == expected
