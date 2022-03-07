import pytest
from dbt.tests.util import run_dbt
from tests.functional.adapter.files import (
    seeds_base_csv,
    seeds_added_csv,
    schema_base_yml,
    incremental_sql,
)
from dbt.tests.adapter import check_relations_equal, relation_from_name


@pytest.fixture(scope="class")
def project_config_update():
    return {"name": "incremental"}


@pytest.fixture(scope="class")
def models():
    return {"incremental.sql": incremental_sql, "schema.yml": schema_base_yml}


@pytest.fixture(scope="class")
def seeds():
    return {"base.csv": seeds_base_csv, "added.csv": seeds_added_csv}


def test_incremental(project):
    # seed command
    results = run_dbt(["seed"])
    assert len(results) == 2

    # base table rowcount
    relation = relation_from_name(project.adapter, "base")
    result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
    assert result[0] == 10

    # added table rowcount
    relation = relation_from_name(project.adapter, "added")
    result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
    assert result[0] == 20

    # run command
    # the "seed_name" var changes the seed identifier in the schema file
    results = run_dbt(["run", "--vars", "seed_name: base"])
    assert len(results) == 1

    # check relations equal
    check_relations_equal(project.adapter, ["base", "incremental"])

    # change seed_name var
    # the "seed_name" var changes the seed identifier in the schema file
    results = run_dbt(["run", "--vars", "seed_name: added"])
    assert len(results) == 1

    # check relations equal
    check_relations_equal(project.adapter, ["added", "incremental"])

    # get catalog from docs generate
    catalog = run_dbt(["docs", "generate"])
    assert len(catalog.nodes) == 3
    assert len(catalog.sources) == 1
