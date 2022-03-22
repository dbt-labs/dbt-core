import pytest
from dbt.tests.util import run_dbt, get_manifest


model_sql = """
  select 1 as id
"""


@pytest.fixture(scope="class")
def models():
    return {"model.sql": model_sql}


@pytest.fixture(scope="class")
def dbt_profile_data(unique_schema):

    return {
        "config": {"send_anonymous_usage_stats": False},
        "test": {
            "outputs": {
                "default": {
                    "type": "postgres",
                    "threads": 4,
                    "host": "localhost",
                    "port": 5432,
                    "user": "root",
                    "pass": "password",
                    "dbname": "dbtMixedCase",
                    "schema": unique_schema,
                },
            },
            "target": "default",
        },
    }


def test_basic(project_root, project):

    assert project.database == "dbtMixedCase"

    # Tests that a project with a single model works
    results = run_dbt(["run"])
    assert len(results) == 1
    manifest = get_manifest(project_root)
    assert "model.test.model" in manifest.nodes
    # Running a second time works
    results = run_dbt(["run"])
