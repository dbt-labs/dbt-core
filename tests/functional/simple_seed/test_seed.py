import pytest
from dbt.tests.util import run_dbt

# from `test/integration/test_simple_seed`, test_postgres_simple_seed


@pytest.fixture(scope="class")
def project_config_update():
    return {"seeds": {"quote_columns": False}}


@pytest.fixture(scope="class")
def seeds():
    return {"data.csv": "a,b\n1,hello\n2,goodbye"}


def test_simple_seed(project):
    results = run_dbt(["seed"])
    assert len(results) == 1
