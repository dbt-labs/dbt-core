import pytest

from dbt.tests.util import run_dbt

from tests.functional.graph_selection.fixtures import SelectionFixtures


def run_schema_and_assert(project, include, exclude, expected_tests):
    # deps must run before seed
    run_dbt(["deps"])
    run_dbt(["seed"])
    results = run_dbt(["run", "--exclude", "never_selected"])
    assert len(results) == 10

    test_args = ["test"]
    if include:
        test_args += ["--select", include]
    if exclude:
        test_args += ["--exclude", exclude]
    test_results = run_dbt(test_args)

    ran_tests = sorted([test.node.name for test in test_results])
    expected_sorted = sorted(expected_tests)

    assert ran_tests == expected_sorted


class TestSchemaTestGraphSelection(SelectionFixtures):
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "git": "https://github.com/dbt-labs/dbt-integration-project",
                    "revision": "dbt/1.0.0",
                }
            ]
        }

    def test_schema_tests_no_specifiers(self, project):
        run_schema_and_assert(
            project,
            None,
            None,
            [
                "not_null_emails_email",
                "unique_table_model_id",
                "unique_users_id",
                "unique_users_rollup_gender",
            ],
        )

    def test_schema_tests_specify_model(self, project):
        run_schema_and_assert(project, "users", None, ["unique_users_id"])

    def test_schema_tests_specify_tag(self, project):
        run_schema_and_assert(
            project, "tag:bi", None, ["unique_users_id", "unique_users_rollup_gender"]
        )

    def test_schema_tests_specify_model_and_children(self, project):
        run_schema_and_assert(
            project, "users+", None, ["unique_users_id", "unique_users_rollup_gender"]
        )

    def test_schema_tests_specify_tag_and_children(self, project):
        run_schema_and_assert(
            project,
            "tag:base+",
            None,
            ["not_null_emails_email", "unique_users_id", "unique_users_rollup_gender"],
        )

    def test_schema_tests_specify_model_and_parents(self, project):
        run_schema_and_assert(
            project,
            "+users_rollup",
            None,
            ["unique_users_id", "unique_users_rollup_gender"],
        )

    def test_schema_tests_specify_model_and_parents_with_exclude(self, project):
        run_schema_and_assert(project, "+users_rollup", "users_rollup", ["unique_users_id"])

    def test_schema_tests_specify_exclude_only(self, project):
        run_schema_and_assert(
            project,
            None,
            "users_rollup",
            ["not_null_emails_email", "unique_table_model_id", "unique_users_id"],
        )

    def test_schema_tests_specify_model_in_pkg(self, project):
        run_schema_and_assert(
            project,
            "test.users_rollup",
            None,
            # TODO: change this. there's no way to select only direct ancestors
            # atm.
            ["unique_users_rollup_gender"],
        )

    def test_schema_tests_with_glob(self, project):
        run_schema_and_assert(
            project,
            "*",
            "users",
            [
                "not_null_emails_email",
                "unique_table_model_id",
                "unique_users_rollup_gender",
            ],
        )

    def test_schema_tests_dep_package_only(self, project):
        run_schema_and_assert(project, "dbt_integration_project", None, ["unique_table_model_id"])

    def test_schema_tests_model_in_dep_pkg(self, project):
        run_schema_and_assert(
            project,
            "dbt_integration_project.table_model",
            None,
            ["unique_table_model_id"],
        )

    def test_schema_tests_exclude_pkg(self, project):
        run_schema_and_assert(
            project,
            None,
            "dbt_integration_project",
            ["not_null_emails_email", "unique_users_id", "unique_users_rollup_gender"],
        )
