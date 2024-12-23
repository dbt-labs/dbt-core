import pytest

from dbt.artifacts.schemas.results import RunStatus
from dbt.contracts.graph.nodes import SavedQuery
from dbt.tests.util import run_dbt
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
        result = run_dbt(["build"])
        assert len(result.results) == 3

        saved_query_results = (
            result for result in result.results if isinstance(result.node, SavedQuery)
        )
        assert {result.node.name for result in saved_query_results} == {"test_saved_query"}
        assert all("NO-OP" in result.message for result in saved_query_results)
        assert all(result.status == RunStatus.NoOp for result in saved_query_results)
