import pytest
from dbt.tests.util import run_dbt
from tests.functional.adapter.files import (
    seeds_base_csv,
    schema_test_seed_yml,
    base_view_sql,
    base_table_sql,
    schema_base_yml,
    schema_test_view_yml,
    schema_test_table_yml,
)


@pytest.fixture(scope="class")
def project_config_update():
    return {"name": "schema_test"}


@pytest.fixture(scope="class")
def seeds():
    return {
        "base.csv": seeds_base_csv,
        "schema.yml": schema_test_seed_yml,
    }


@pytest.fixture(scope="class")
def models():
    return {
        "view_model.sql": base_view_sql,
        "table_model.sql": base_table_sql,
        "schema.yml": schema_base_yml,
        "schema_view.yml": schema_test_view_yml,
        "schema_table.yml": schema_test_table_yml,
    }


def test_schema_tests(project):
    # seed command
    results = run_dbt(["seed"])

    # test command selecting base model
    results = run_dbt(["test", "-m", "base"])
    assert len(results) == 1

    # run command
    results = run_dbt(["run"])
    assert len(results) == 2

    # test command, all tests
    results = run_dbt(["test"])
    assert len(results) == 3
