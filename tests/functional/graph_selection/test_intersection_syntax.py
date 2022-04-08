import pytest

from dbt.tests.util import run_dbt, check_result_nodes_by_name
from tests.functional.graph_selection.fixtures import SelectionFixtures


selectors_yml = """
            selectors:
            - name: same_intersection
              definition:
                intersection:
                  - fqn: users
                  - fqn: users
            - name: tags_intersection
              definition:
                intersection:
                  - tag: bi
                  - tag: users
            - name: triple_descending
              definition:
                intersection:
                  - fqn: "*"
                  - tag: bi
                  - tag: users
            - name: triple_ascending
              definition:
                intersection:
                  - tag: users
                  - tag: bi
                  - fqn: "*"
            - name: intersection_with_exclusion
              definition:
                intersection:
                  - method: fqn
                    value: users_rollup_dependency
                    parents: true
                  - method: fqn
                    value: users
                    children: true
                  - exclude:
                    - users_rollup_dependency
            - name: intersection_exclude_intersection
              definition:
                intersection:
                  - tag:bi
                  - "@users"
                  - exclude:
                      - intersection:
                        - tag:bi
                        - method: fqn
                          value: users_rollup
                          children: true
            - name: intersection_exclude_intersection_lack
              definition:
                intersection:
                  - tag:bi
                  - "@users"
                  - exclude:
                      - intersection:
                        - method: fqn
                          value: emails
                          children_parents: true
                        - method: fqn
                          value: emails_alt
                          children_parents: true
        """


# The project and run_seed fixtures will be executed for each test method
class TestIntersectionSyncs(SelectionFixtures):
    # The tests here aiming to test whether the correct node is selected,
    # we don't need the run to pass
    @pytest.fixture(scope="class")
    def selectors(self):
        return selectors_yml

    def test_same_model_intersection(self, project):
        results = run_dbt(["run", "--models", "users,users"], expect_pass=False)
        check_result_nodes_by_name(results, ["users"])

    def test_same_model_intersection_selectors(self, project):

        results = run_dbt(["run", "--selector", "same_intersection"], expect_pass=False)
        check_result_nodes_by_name(results, ["users"])

    def test_tags_intersection(self, project):

        results = run_dbt(["run", "--models", "tag:bi,tag:users"], expect_pass=False)
        check_result_nodes_by_name(results, ["users"])

    def test_tags_intersection_selectors(self, project):

        results = run_dbt(["run", "--selector", "tags_intersection"], expect_pass=False)
        check_result_nodes_by_name(results, ["users"])

    def test_intersection_triple_descending(self, project):

        results = run_dbt(["run", "--models", "*,tag:bi,tag:users"], expect_pass=False)
        check_result_nodes_by_name(results, ["users"])

    def test_intersection_triple_descending_schema(self, project):

        results = run_dbt(["run", "--models", "*,tag:bi,tag:users"], expect_pass=False)
        check_result_nodes_by_name(results, ["users"])

    def test_intersection_triple_descending_schema_selectors(self, project):

        results = run_dbt(["run", "--selector", "triple_descending"], expect_pass=False)
        check_result_nodes_by_name(results, ["users"])

    def test_intersection_triple_ascending(self, project):

        results = run_dbt(["run", "--models", "tag:users,tag:bi,*"], expect_pass=False)
        check_result_nodes_by_name(results, ["users"])

    def test_intersection_triple_ascending_schema_selectors(self, project):

        results = run_dbt(["run", "--selector", "triple_ascending"], expect_pass=False)
        check_result_nodes_by_name(results, ["users"])

    def test_intersection_with_exclusion(self, project):

        results = run_dbt(
            [
                "run",
                "--models",
                "+users_rollup_dependency,users+",
                "--exclude",
                "users_rollup_dependency",
            ],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["users", "users_rollup"])

    def test_intersection_with_exclusion_selectors(self, project):

        results = run_dbt(["run", "--selector", "intersection_with_exclusion"], expect_pass=False)
        check_result_nodes_by_name(results, ["users", "users_rollup"])

    def test_intersection_exclude_intersection(self, project):

        results = run_dbt(
            ["run", "--models", "tag:bi,@users", "--exclude", "tag:bi,users_rollup+"],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["users"])

    def test_intersection_exclude_intersection_selectors(self, project):

        results = run_dbt(
            ["run", "--selector", "intersection_exclude_intersection"],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["users"])

    def test_intersection_exclude_intersection_lack(self, project):

        results = run_dbt(
            ["run", "--models", "tag:bi,@users", "--exclude", "@emails,@emails_alt"],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["users", "users_rollup"])

    def test_intersection_exclude_intersection_lack_selector(self, project):

        results = run_dbt(
            ["run", "--selector", "intersection_exclude_intersection_lack"],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["users", "users_rollup"])

    def test_intersection_exclude_triple_intersection(self, project):

        results = run_dbt(
            ["run", "--models", "tag:bi,@users", "--exclude", "*,tag:bi,users_rollup"],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["users"])

    def test_intersection_concat(self, project):

        results = run_dbt(["run", "--models", "tag:bi,@users", "emails_alt"], expect_pass=False)
        check_result_nodes_by_name(results, ["users", "users_rollup", "emails_alt"])

    def test_intersection_concat_intersection(self, project):

        results = run_dbt(
            ["run", "--models", "tag:bi,@users", "@emails_alt,emails_alt"],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["users", "users_rollup", "emails_alt"])

    def test_intersection_concat_exclude(self, project):

        results = run_dbt(
            [
                "run",
                "--models",
                "tag:bi,@users",
                "emails_alt",
                "--exclude",
                "users_rollup",
            ],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["users", "emails_alt"])

    def test_intersection_concat_exclude_concat(self, project):

        results = run_dbt(
            [
                "run",
                "--models",
                "tag:bi,@users",
                "emails_alt,@users",
                "--exclude",
                "users_rollup_dependency",
                "users_rollup",
            ],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["users", "emails_alt"])

    def test_intersection_concat_exclude_intersection_concat(self, project):

        results = run_dbt(
            [
                "run",
                "--models",
                "tag:bi,@users",
                "emails_alt,@users",
                "--exclude",
                "@users,users_rollup_dependency",
                "@users,users_rollup",
            ],
            expect_pass=False,
        )
        check_result_nodes_by_name(results, ["users", "emails_alt"])
