import pytest

from dbt.artifacts.resources import SemanticQueryArgs
from dbt.tests.util import run_dbt
from dbt_common.exceptions import CompilationError, DbtRuntimeError
from tests.functional.semantic_queries.fixtures import (
    customers_sql,
    metricflow_time_spine_sql,
    orders_sql,
    saved_query_model_sql,
    semantic_layer_yml,
    semantic_query_analysis_sql,
    semantic_query_model_sql,
    semantic_query_singular_test_sql,
)


class SemanticQueryProject:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders.sql": orders_sql,
            "customers.sql": customers_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "semantic_layer.yml": semantic_layer_yml,
            "uses_semantic_query.sql": semantic_query_model_sql,
            "uses_saved_query.sql": saved_query_model_sql,
        }


class TestSemanticQueryRecordedAtParse(SemanticQueryProject):
    def test_semantic_query_is_recorded(self, project):
        manifest = run_dbt(["parse"])
        node = manifest.nodes["model.test.uses_semantic_query"]
        assert node.semantic_queries == [
            SemanticQueryArgs(
                metrics=["revenue"],
                group_by=["customer__region"],
                where=["{{ Dimension('customer__region') }} = 'west'"],
            )
        ]


class TestSavedQueryRecordedAtParse(SemanticQueryProject):
    def test_saved_query_is_recorded(self, project):
        manifest = run_dbt(["parse"])
        node = manifest.nodes["model.test.uses_saved_query"]
        assert node.semantic_queries == [SemanticQueryArgs(saved_query="revenue_by_region")]


class TestNodesWithoutQueriesRecordNothing(SemanticQueryProject):
    def test_no_queries_recorded(self, project):
        manifest = run_dbt(["parse"])
        assert manifest.nodes["model.test.orders"].semantic_queries == []


class TestSemanticQueryRecordedInAnalysis(SemanticQueryProject):
    @pytest.fixture(scope="class")
    def analyses(self):
        return {"total_revenue.sql": semantic_query_analysis_sql}

    def test_semantic_query_is_recorded(self, project):
        manifest = run_dbt(["parse"])
        node = manifest.nodes["analysis.test.total_revenue"]
        assert node.semantic_queries == [SemanticQueryArgs(metrics=["revenue"])]


class TestSemanticQueryNotAvailableInSingularTests(SemanticQueryProject):
    @pytest.fixture(scope="class")
    def tests(self):
        return {"revenue_check.sql": semantic_query_singular_test_sql}

    def test_parsing_fails(self, project):
        with pytest.raises(CompilationError, match="only available in models and analyses"):
            run_dbt(["parse"])


class TestSemanticQueryCanNotYetBeCompiled(SemanticQueryProject):
    def test_compiling_fails_clearly(self, project):
        with pytest.raises(DbtRuntimeError, match=r"metric_query\(\) can not yet be compiled"):
            run_dbt(["compile", "--select", "uses_semantic_query"])
