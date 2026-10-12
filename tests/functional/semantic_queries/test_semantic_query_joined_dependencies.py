import pytest

from dbt.exceptions import ParsingError
from dbt.tests.util import get_manifest, run_dbt, write_file
from tests.functional.semantic_queries.fixtures import (
    customers_sql,
    metric_query_metric_only_model_sql,
    metric_query_unknown_dimension_model_sql,
    metric_query_where_only_model_sql,
    metricflow_time_spine_sql,
    orders_sql,
    saved_query_model_sql,
    semantic_layer_yml,
    semantic_query_model_sql,
)

ORDERS = "semantic_model.test.orders"
CUSTOMERS = "semantic_model.test.customers"


class SemanticQueryProject:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders.sql": orders_sql,
            "customers.sql": customers_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "semantic_layer.yml": semantic_layer_yml,
            "joins_via_group_by.sql": semantic_query_model_sql,
            "joins_via_where.sql": metric_query_where_only_model_sql,
            "joins_via_saved_query.sql": saved_query_model_sql,
            "no_join.sql": metric_query_metric_only_model_sql,
        }


class TestGroupByOnJoinedDimensionDependsOnItsSemanticModel(SemanticQueryProject):
    def test_depends_on_joined_semantic_model(self, project):
        manifest = run_dbt(["parse"])
        depends_on = manifest.nodes["model.test.joins_via_group_by"].depends_on.nodes
        assert ORDERS in depends_on
        assert CUSTOMERS in depends_on


class TestWhereOnJoinedDimensionDependsOnItsSemanticModel(SemanticQueryProject):
    def test_depends_on_joined_semantic_model(self, project):
        manifest = run_dbt(["parse"])
        depends_on = manifest.nodes["model.test.joins_via_where"].depends_on.nodes
        assert ORDERS in depends_on
        assert CUSTOMERS in depends_on


class TestSavedQueryOnJoinedDimensionDependsOnItsSemanticModel(SemanticQueryProject):
    def test_depends_on_joined_semantic_model(self, project):
        manifest = run_dbt(["parse"])
        depends_on = manifest.nodes["model.test.joins_via_saved_query"].depends_on.nodes
        assert ORDERS in depends_on
        assert CUSTOMERS in depends_on


class TestQueryWithoutJoinsDoesNotDependOnUnusedSemanticModels(SemanticQueryProject):
    def test_only_depends_on_the_metrics_semantic_model(self, project):
        manifest = run_dbt(["parse"])
        depends_on = manifest.nodes["model.test.no_join"].depends_on.nodes
        assert ORDERS in depends_on
        assert CUSTOMERS not in depends_on


class TestJoinedModelsAreOrderedBeforeTheQueryingNode(SemanticQueryProject):
    def test_joined_models_are_ancestors(self, project):
        results = run_dbt(["list", "--select", "+joins_via_where", "--resource-type", "model"])
        assert "test.orders" in results
        assert "test.customers" in results


class TestQueryOfUnknownDimension(SemanticQueryProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders.sql": orders_sql,
            "customers.sql": customers_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "semantic_layer.yml": semantic_layer_yml,
            "bad_query.sql": metric_query_unknown_dimension_model_sql,
        }

    def test_parsing_fails(self, project):
        with pytest.raises(ParsingError, match="Invalid semantic query in model 'bad_query'"):
            run_dbt(["parse"])


class TestQueryNoLongerJoiningDropsTheDependency(SemanticQueryProject):
    def test_stale_dependency_is_removed(self, project):
        manifest = run_dbt(["parse"])
        assert CUSTOMERS in manifest.nodes["model.test.joins_via_where"].depends_on.nodes

        write_file(
            metric_query_metric_only_model_sql,
            project.project_root,
            "models",
            "joins_via_where.sql",
        )
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        depends_on = manifest.nodes["model.test.joins_via_where"].depends_on.nodes
        assert ORDERS in depends_on
        assert CUSTOMERS not in depends_on


class TestChangingTheSemanticLayerRevalidatesQueries(SemanticQueryProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders.sql": orders_sql,
            "customers.sql": customers_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "semantic_layer.yml": semantic_layer_yml,
            "joins_via_group_by.sql": semantic_query_model_sql,
        }

    def test_node_is_rechecked_when_only_the_semantic_layer_changes(self, project):
        run_dbt(["parse"])

        # Rename the dimension the model's query groups by. The model itself is unchanged, but
        # its query can no longer be answered. The saved query is dropped so that the semantic
        # layer stays valid on its own.
        renamed = semantic_layer_yml.split("saved_queries:")[0].replace(
            "name: region", "name: area"
        )
        write_file(renamed, project.project_root, "models", "semantic_layer.yml")
        with pytest.raises(
            ParsingError, match="Invalid semantic query in model 'joins_via_group_by'"
        ):
            run_dbt(["parse"])
