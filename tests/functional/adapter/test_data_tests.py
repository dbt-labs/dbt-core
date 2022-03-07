import pytest
from tests.functional.adapter.files import (
    test_passing_sql,
    test_failing_sql,
)
from dbt.tests.adapter import check_result_nodes_by_name
from dbt.tests.util import run_dbt


@pytest.fixture(scope="class")
def tests():
    return {
        "passing.sql": test_passing_sql,
        "failing.sql": test_failing_sql,
    }


@pytest.fixture(scope="class")
def project_config_update():
    return {"name": "data_tests"}


def test_data_tests(project):
    # test command
    results = run_dbt(["test"])
    assert len(results) == 2

    # We have the right result nodes
    check_result_nodes_by_name(results, ["passing", "failing"])

    # Check result status
    for result in results:
        if result.node.name == "passing":
            assert result.status == "pass"
        elif result.node.name == "failing":
            assert result.status == "fail"
