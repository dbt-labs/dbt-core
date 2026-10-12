import pytest

from dbt.exceptions import TargetNotFoundError
from dbt.tests.util import get_manifest, run_dbt, write_file
from dbt_common.exceptions import CompilationError
from tests.functional.semantic_queries.fixtures import (
    customers_sql,
    metricflow_time_spine_sql,
    orders_sql,
    saved_query_model_sql,
    semantic_layer_yml,
    semantic_query_model_sql,
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


class TestSemanticQueryDependsOnItsMetrics(SemanticQueryProject):
    def test_depends_on_metric(self, project):
        manifest = run_dbt(["parse"])
        node = manifest.nodes["model.test.uses_semantic_query"]
        assert "metric.test.revenue" in node.depends_on.nodes


class TestSavedQueryDependsOnItsMetrics(SemanticQueryProject):
    def test_depends_on_metrics_but_not_the_saved_query(self, project):
        manifest = run_dbt(["parse"])
        node = manifest.nodes["model.test.uses_saved_query"]
        assert "metric.test.revenue" in node.depends_on.nodes
        assert "saved_query.test.revenue_by_region" not in node.depends_on.nodes


class TestSemanticQueryOrdersNodeAfterUpstreamModels(SemanticQueryProject):
    def test_upstream_models_are_ancestors(self, project):
        results = run_dbt(["list", "--select", "+uses_semantic_query", "--resource-type", "model"])
        assert "test.orders" in results
        assert "test.uses_semantic_query" in results


class TestSemanticQueryOfUnknownMetric(SemanticQueryProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders.sql": orders_sql,
            "customers.sql": customers_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "semantic_layer.yml": semantic_layer_yml,
            "uses_unknown_metric.sql": "select * from ({{ metric_query(metrics='nope') }}) q",
        }

    def test_parsing_fails(self, project):
        with pytest.raises(TargetNotFoundError, match="nope"):
            run_dbt(["parse"])


class TestSavedQueryOfUnknownSavedQuery(SemanticQueryProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders.sql": orders_sql,
            "customers.sql": customers_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "semantic_layer.yml": semantic_layer_yml,
            "uses_unknown_saved_query.sql": "select * from ({{ saved_query('nope') }}) q",
        }

    def test_parsing_fails(self, project):
        with pytest.raises(TargetNotFoundError, match="nope"):
            run_dbt(["parse"])


class TestSemanticQueryOnUpstreamModelIsACycle(SemanticQueryProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            # `orders` is the model behind the semantic model the metric is measured on
            "orders.sql": "select * from ({{ metric_query(metrics='revenue') }}) q",
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "semantic_layer.yml": semantic_layer_yml,
            "customers.sql": customers_sql,
        }

    def test_graph_has_a_cycle(self, project):
        with pytest.raises(CompilationError, match="Found a cycle"):
            run_dbt(["compile"])


class TestRemovingAQueriedMetricFailsPartialParsing(SemanticQueryProject):
    def test_node_is_reparsed_when_its_metric_is_removed(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "metric.test.revenue" in manifest.metrics

        write_file(
            semantic_layer_yml.replace("name: revenue\n    label", "name: sales\n    label"),
            project.project_root,
            "models",
            "semantic_layer.yml",
        )
        with pytest.raises(TargetNotFoundError, match="revenue"):
            run_dbt(["parse"])
