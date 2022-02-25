import os
import json
import pytest

from dbt.tests.util import run_dbt
from dbt.tests.tables import TableComparison
from tests.functional.graph_selection.fixtures import models, seeds  # noqa


selectors_yml = """
            selectors:
            - name: bi_selector
              description: This is a BI selector
              definition:
                method: tag
                value: bi
        """


@pytest.fixture
def selectors():
    return selectors_yml


def assert_correct_schemas(project, table_comp):
    with table_comp.get_connection():
        exists = project.adapter.check_schema_exists(project.database, project.test_schema)
        assert exists

        schema = project.test_schema + "_and_then"
        exists = project.adapter.check_schema_exists(project.database, schema)
        assert not exists


def test_specific_model(project):
    run_dbt(["seed"])
    results = run_dbt(["run", "--select", "users"])
    assert len(results) == 1

    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("seed", "users")

    created_tables = project.get_tables_in_schema()
    assert "users_rollup" not in created_tables
    assert "alternative.users" not in created_tables
    assert "base_users" not in created_tables
    assert "emails" not in created_tables
    assert_correct_schemas(project, table_comp)


def test_tags(project, project_root):
    run_dbt(["seed"])
    results = run_dbt(["run", "--selector", "bi_selector"])
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    assert len(results) == 2
    created_tables = project.get_tables_in_schema()
    assert not ("alternative.users" in created_tables)
    assert not ("base_users" in created_tables)
    assert not ("emails" in created_tables)
    assert "users" in created_tables
    assert "users_rollup" in created_tables
    assert_correct_schemas(project, table_comp)
    manifest_path = project_root.join("target/manifest.json")
    assert os.path.exists(manifest_path)
    with open(manifest_path) as fp:
        manifest = json.load(fp)
        assert "selectors" in manifest


def test_tags_and_children(project):
    run_dbt(["seed"])
    results = run_dbt(["run", "--select", "tag:base+"])
    assert len(results) == 5
    created_models = project.get_tables_in_schema()
    assert not ("base_users" in created_models)
    assert not ("emails" in created_models)
    assert "emails_alt" in created_models
    assert "users_rollup" in created_models
    assert "users" in created_models
    assert "alternative.users" in created_models
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    assert_correct_schemas(project, table_comp)


def test_tags_and_children_limited(project):
    run_dbt(["seed"])
    results = run_dbt(["run", "--select", "tag:base+2"])
    assert len(results) == 4
    created_models = project.get_tables_in_schema()
    assert not ("base_users" in created_models)
    assert not ("emails" in created_models)
    assert "emails_alt" in created_models
    assert "users_rollup" in created_models
    assert "users" in created_models
    assert "alternative.users" in created_models
    assert "users_rollup_dependency" not in created_models
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    assert_correct_schemas(project, table_comp)


def test_specific_model_and_children(project):
    run_dbt(["seed"])
    results = run_dbt(["run", "--select", "users+"])
    assert len(results) == 4
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("seed", "users")
    table_comp.assert_tables_equal("summary_expected", "users_rollup")

    created_models = project.get_tables_in_schema()
    assert "emails_alt" in created_models
    assert "base_users" not in created_models
    assert "alternative.users" not in created_models
    assert "emails" not in created_models
    assert_correct_schemas(project, table_comp)


def test_specific_model_and_children_limited(project):
    run_dbt(["seed"])
    results = run_dbt(["run", "--select", "users+1"])
    assert len(results) == 3
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("seed", "users")
    table_comp.assert_tables_equal("summary_expected", "users_rollup")

    created_models = project.get_tables_in_schema()
    assert "emails_alt" in created_models
    assert "base_users" not in created_models
    assert "emails" not in created_models
    assert "users_rollup_dependency" not in created_models
    assert_correct_schemas(project, table_comp)


def test_specific_model_and_parents(project):
    run_dbt(["seed"])
    results = run_dbt(["run", "--select", "+users_rollup"])
    assert len(results) == 2
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("seed", "users")
    table_comp.assert_tables_equal("summary_expected", "users_rollup")

    created_models = project.get_tables_in_schema()
    assert not ("base_users" in created_models)
    assert not ("emails" in created_models)
    assert_correct_schemas(project, table_comp)


def test_specific_model_and_parents_limited(project):
    run_dbt(["seed"])
    results = run_dbt(["run", "--select", "1+users_rollup"])
    assert len(results) == 2
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("seed", "users")
    table_comp.assert_tables_equal("summary_expected", "users_rollup")

    created_models = project.get_tables_in_schema()
    assert not ("base_users" in created_models)
    assert not ("emails" in created_models)
    assert_correct_schemas(project, table_comp)


def test_specific_model_with_exclusion(project):
    run_dbt(["seed"])
    results = run_dbt(["run", "--select", "+users_rollup", "--exclude", "models/users_rollup.sql"])
    assert len(results) == 1

    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("seed", "users")

    created_models = project.get_tables_in_schema()
    assert not ("base_users" in created_models)
    assert not ("users_rollup" in created_models)
    assert not ("emails" in created_models)
    assert_correct_schemas(project, table_comp)


def test_locally_qualified_name(project):
    run_dbt(["seed"])
    results = run_dbt(["run", "--select", "test.subdir"])
    assert len(results) == 2
    created_models = project.get_tables_in_schema()
    assert "users_rollup" not in created_models
    assert "base_users" not in created_models
    assert "emails" not in created_models
    assert "subdir" in created_models
    assert "nested_users" in created_models
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    assert_correct_schemas(project, table_comp)

    results = run_dbt(["run", "--select", "models/test/subdir*"])
    assert len(results) == 2
    created_models = project.get_tables_in_schema()
    assert "users_rollup" not in created_models
    assert "base_users" not in created_models
    assert "emails" not in created_models
    assert "subdir" in created_models
    assert "nested_users" in created_models
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    assert_correct_schemas(project, table_comp)


def test_locally_qualified_name_model_with_dots(project):
    run_dbt(["seed"])
    results = run_dbt(["run", "--select", "alternative.users"])
    assert len(results) == 1
    created_models = project.get_tables_in_schema()
    assert "alternative.users" in created_models
    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    assert_correct_schemas(project, table_comp)

    results = run_dbt(["run", "--select", "models/alternative.*"])
    assert len(results) == 1
    created_models = project.get_tables_in_schema()
    assert "alternative.users" in created_models
    assert_correct_schemas(project, table_comp)


def test_childrens_parents(project):
    run_dbt(["seed"])
    results = run_dbt(["run", "--select", "@base_users"])
    assert len(results) == 5
    created_models = project.get_tables_in_schema()
    assert "users_rollup" in created_models
    assert "users" in created_models
    assert "emails_alt" in created_models
    assert "alternative.users" in created_models
    assert "subdir" not in created_models
    assert "nested_users" not in created_models

    results = run_dbt(["test", "--select", "test_name:not_null"])
    assert len(results) == 1
    assert results[0].node.name == "not_null_emails_email"


def test_more_childrens_parents(project):
    run_dbt(["seed"])
    results = run_dbt(["run", "--select", "@users"])
    assert len(results) == 4
    created_models = project.get_tables_in_schema()
    assert "users_rollup" in created_models
    assert "users" in created_models
    assert "emails_alt" in created_models
    assert "subdir" not in created_models
    assert "nested_users" not in created_models
    results = run_dbt(["test", "--select", "test_name:unique"])
    assert len(results) == 2
    assert sorted([r.node.name for r in results]) == [
        "unique_users_id",
        "unique_users_rollup_gender",
    ]


def test_concat(project):
    run_dbt(["seed"])
    results = run_dbt(["run", "--select", "@emails_alt", "users_rollup"])
    assert len(results) == 3
    created_models = project.get_tables_in_schema()
    assert "users_rollup" in created_models
    assert "users" in created_models
    assert "emails_alt" in created_models
    assert "subdir" not in created_models
    assert "nested_users" not in created_models


def test_concat_exclude(project):
    run_dbt(["seed"])
    results = run_dbt(
        ["run", "--select", "@emails_alt", "users_rollup", "--exclude", "emails_alt"]
    )
    assert len(results) == 2
    created_models = project.get_tables_in_schema()
    assert "users" in created_models
    assert "users_rollup" in created_models
    assert "emails_alt" not in created_models
    assert "subdir" not in created_models
    assert "nested_users" not in created_models


def test_concat_exclude_concat(project):
    run_dbt(["seed"])
    results = run_dbt(
        [
            "run",
            "--select",
            "@emails_alt",
            "users_rollup",
            "--exclude",
            "emails_alt",
            "users_rollup",
        ]
    )
    assert len(results) == 1
    created_models = project.get_tables_in_schema()
    assert "users" in created_models
    assert "emails_alt" not in created_models
    assert "users_rollup" not in created_models
    assert "subdir" not in created_models
    assert "nested_users" not in created_models
    results = run_dbt(
        [
            "test",
            "--select",
            "@emails_alt",
            "users_rollup",
            "--exclude",
            "emails_alt",
            "users_rollup",
        ]
    )
    assert len(results) == 1
    assert results[0].node.name == "unique_users_id"


def test_exposure_parents(project):
    run_dbt(["seed"])
    results = run_dbt(["ls", "--select", "+exposure:seed_ml_exposure"])
    assert len(results) == 2
    assert sorted(results) == ["exposure:test.seed_ml_exposure", "source:test.raw.seed"]
    results = run_dbt(["ls", "--select", "1+exposure:user_exposure"])
    assert len(results) == 5
    assert sorted(results) == [
        "exposure:test.user_exposure",
        "test.unique_users_id",
        "test.unique_users_rollup_gender",
        "test.users",
        "test.users_rollup",
    ]
    results = run_dbt(["run", "-m", "+exposure:user_exposure"])
    assert len(results) == 2
    created_models = project.get_tables_in_schema()
    assert "users_rollup" in created_models
    assert "users" in created_models
    assert "emails_alt" not in created_models
    assert "subdir" not in created_models
    assert "nested_users" not in created_models
