import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture
from tests.functional.saved_queries.fixtures import (
    saved_queries_yml,
    saved_query_description,
)
from tests.functional.semantic_models.fixtures import (
    fct_revenue_sql,
    metricflow_time_spine_sql,
    schema_yml,
)


class TestSavedQueryBuild:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "saved_queries.yml": saved_queries_yml,
            "schema.yml": schema_yml,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "docs.md": saved_query_description,
        }

    @pytest.fixture(scope="class")
    def packages(self):
        return """
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
"""

    def test_build_saved_queries_no_op(self, project) -> None:
        """Test building saved query exports with no flag, so should be no-op."""
        run_dbt(["deps"])
        result, log_output = run_dbt_and_capture(["build", "--log-format", "json"])
        assert len(result.results) == 3
        assert "NO-OP" in [r.message for r in result.results]

        result_log_line = next(
            line for line in log_output.split("\n") if "LogNodeNoOpResult" in line
        )
        assert "my_group" in result_log_line
        assert "group_owner" in result_log_line
