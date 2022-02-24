import pytest

from dbt.tests.util import run_dbt

from tests.functional.graph_selection.fixtures import models, create_tables, seeds  # noqa


def run_schema_and_assert(project, include, exclude, expected_tests):
    # run_dbt(['seed'])
    run_dbt(["deps"])
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


@pytest.fixture
def packages():
    return {
        "packages": [
            {
                "git": "https://github.com/dbt-labs/dbt-integration-project",
                "revision": "dbt/1.0.0",
            }
        ]
    }


def test__postgres__schema_tests_no_specifiers(project, create_tables):  # noqa
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


def test__postgres__schema_tests_specify_model(project, create_tables):  # noqa
    run_schema_and_assert(project, "users", None, ["unique_users_id"])


def test__postgres__schema_tests_specify_tag(project, create_tables):  # noqa
    run_schema_and_assert(
        project, "tag:bi", None, ["unique_users_id", "unique_users_rollup_gender"]
    )


def test__postgres__schema_tests_specify_model_and_children(project, create_tables):  # noqa
    run_schema_and_assert(
        project, "users+", None, ["unique_users_id", "unique_users_rollup_gender"]
    )


def test__postgres__schema_tests_specify_tag_and_children(project, create_tables):  # noqa
    run_schema_and_assert(
        project,
        "tag:base+",
        None,
        ["not_null_emails_email", "unique_users_id", "unique_users_rollup_gender"],
    )


def test__postgres__schema_tests_specify_model_and_parents(project, create_tables):  # noqa
    run_schema_and_assert(
        project, "+users_rollup", None, ["unique_users_id", "unique_users_rollup_gender"]
    )


def test__postgres__schema_tests_specify_model_and_parents_with_exclude(
    project, create_tables  # noqa
):
    run_schema_and_assert(project, "+users_rollup", "users_rollup", ["unique_users_id"])


def test__postgres__schema_tests_specify_exclude_only(project, create_tables):  # noqa
    run_schema_and_assert(
        project,
        None,
        "users_rollup",
        ["not_null_emails_email", "unique_table_model_id", "unique_users_id"],
    )


def test__postgres__schema_tests_specify_model_in_pkg(project, create_tables):  # noqa
    run_schema_and_assert(
        project,
        "test.users_rollup",
        None,
        # TODO: change this. there's no way to select only direct ancestors
        # atm.
        ["unique_users_rollup_gender"],
    )


def test__postgres__schema_tests_with_glob(project, create_tables):  # noqa
    run_schema_and_assert(
        project,
        "*",
        "users",
        ["not_null_emails_email", "unique_table_model_id", "unique_users_rollup_gender"],
    )


def test__postgres__schema_tests_dep_package_only(project, create_tables):  # noqa
    run_schema_and_assert(project, "dbt_integration_project", None, ["unique_table_model_id"])


def test__postgres__schema_tests_model_in_dep_pkg(project, create_tables):  # noqa
    run_schema_and_assert(
        project, "dbt_integration_project.table_model", None, ["unique_table_model_id"]
    )


def test__postgres__schema_tests_exclude_pkg(project, create_tables):  # noqa
    run_schema_and_assert(
        project,
        None,
        "dbt_integration_project",
        ["not_null_emails_email", "unique_users_id", "unique_users_rollup_gender"],
    )
