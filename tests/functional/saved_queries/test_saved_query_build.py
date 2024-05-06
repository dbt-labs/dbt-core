import pytest

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


class TestSavedQueryBuildNoOp:
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

    def test_build_saved_queries(self, project):
        run_dbt(["deps"])
        result = run_dbt(["build"])
        assert len(result.results) == 3
        assert "NO-OP" in [r.message for r in result.results]
