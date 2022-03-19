import pytest

from dbt.tests.util import run_dbt, check_result_nodes_by_name
from dbt.tests.adapter.basic.files import (
    seeds_base_csv,
    ephemeral_with_cte_sql,
    test_ephemeral_passing_sql,
    test_ephemeral_failing_sql,
    schema_base_yml,
)


class BaseSingularTestsEphemeral:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral.sql": ephemeral_with_cte_sql,
            "passing_model.sql": test_ephemeral_passing_sql,
            "failing_model.sql": test_ephemeral_failing_sql,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "passing.sql": test_ephemeral_passing_sql,
            "failing.sql": test_ephemeral_failing_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "singular_tests_ephemeral",
        }

    def test_singular_tests_ephemeral(self, project):
        # check results from seed command
        results = run_dbt(["seed"])
        assert len(results) == 1
        check_result_nodes_by_name(results, ["base"])

        # Check results from test command
        results = run_dbt(["test"])
        assert len(results) == 2
        check_result_nodes_by_name(results, ["passing", "failing"])

        # Check result status
        for result in results:
            if result.node.name == "passing":
                assert result.status == "pass"
            elif result.node.name == "failing":
                assert result.status == "fail"

        # check results from run command
        results = run_dbt()
        assert len(results) == 2
        check_result_nodes_by_name(results, ["failing_model", "passing_model"])


class TestSingularTestsEphemeral(BaseSingularTestsEphemeral):
    pass
