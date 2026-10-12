from unittest import mock

import pytest

from dbt.artifacts.resources import SemanticQueryArgs
from dbt.context.providers import (
    PARSE_TIME_SEMANTIC_QUERY_SQL,
    ParseMetricQueryResolver,
    ParseSavedQueryResolver,
    RuntimeMetricQueryResolver,
    RuntimeSavedQueryResolver,
)
from dbt.node_types import NodeType
from dbt_common.exceptions import CompilationError


def make_model(resource_type: NodeType = NodeType.Model):
    model = mock.MagicMock()
    model.resource_type = resource_type
    model.name = "my_node"
    model.semantic_queries = []
    return model


def make_resolver(resolver_cls, model=None):
    return resolver_cls(
        mock.MagicMock(), model or make_model(), mock.MagicMock(), mock.MagicMock()
    )


class TestParseMetricQueryResolver:
    def test_records_query_and_returns_placeholder(self):
        model = make_model()
        resolver = make_resolver(ParseMetricQueryResolver, model)

        result = resolver(metrics=["revenue"], group_by=["customer__region"], where="x = 1")

        assert result == PARSE_TIME_SEMANTIC_QUERY_SQL
        assert model.semantic_queries == [
            SemanticQueryArgs(metrics=["revenue"], group_by=["customer__region"], where=["x = 1"])
        ]

    def test_comma_separated_strings_are_split_for_metrics_and_group_by(self):
        model = make_model()
        resolver = make_resolver(ParseMetricQueryResolver, model)

        resolver(metrics="revenue, orders", group_by="a,b")

        assert model.semantic_queries[0].metrics == ["revenue", "orders"]
        assert model.semantic_queries[0].group_by == ["a", "b"]

    def test_where_is_not_split_on_commas(self):
        model = make_model()
        resolver = make_resolver(ParseMetricQueryResolver, model)

        resolver(metrics="revenue", where="region in ('west', 'east')")

        assert model.semantic_queries[0].where == ["region in ('west', 'east')"]

    def test_identical_queries_are_recorded_once(self):
        model = make_model()
        resolver = make_resolver(ParseMetricQueryResolver, model)

        resolver(metrics="revenue")
        resolver(metrics=["revenue"])

        assert len(model.semantic_queries) == 1

    def test_query_only_arguments_are_validated_but_not_recorded(self):
        model = make_model()
        resolver = make_resolver(ParseMetricQueryResolver, model)

        resolver(metrics="revenue", order_by="-revenue", limit=5, start_time="2024-01-01")

        assert model.semantic_queries == [SemanticQueryArgs(metrics=["revenue"])]

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"metrics": []},
            {"metrics": ""},
            {"metrics": 5},
            {"metrics": ["revenue", 5]},
            {"metrics": "revenue", "group_by": 5},
            {"metrics": "revenue", "where": {"a": 1}},
            {"metrics": "revenue", "limit": "10"},
            {"metrics": "revenue", "limit": True},
            {"metrics": "revenue", "start_time": 2024},
            {"metrics": "revenue", "end_time": 2024},
        ],
    )
    def test_invalid_arguments_raise(self, kwargs):
        resolver = make_resolver(ParseMetricQueryResolver)
        with pytest.raises(CompilationError):
            resolver(**kwargs)

    @pytest.mark.parametrize("resource_type", [NodeType.Model, NodeType.Analysis])
    def test_available_in_models_and_analyses(self, resource_type):
        resolver = make_resolver(ParseMetricQueryResolver, make_model(resource_type))
        resolver(metrics="revenue")

    @pytest.mark.parametrize(
        "resource_type",
        [NodeType.Test, NodeType.Snapshot, NodeType.Seed, NodeType.Operation, NodeType.Unit],
    )
    def test_not_available_elsewhere(self, resource_type):
        resolver = make_resolver(ParseMetricQueryResolver, make_model(resource_type))
        with pytest.raises(CompilationError, match="only available in models and analyses"):
            resolver(metrics="revenue")


class TestParseSavedQueryResolver:
    def test_records_query_and_returns_placeholder(self):
        model = make_model()
        resolver = make_resolver(ParseSavedQueryResolver, model)

        assert resolver("revenue_by_region") == PARSE_TIME_SEMANTIC_QUERY_SQL
        assert model.semantic_queries == [SemanticQueryArgs(saved_query="revenue_by_region")]

    def test_name_must_be_a_string(self):
        resolver = make_resolver(ParseSavedQueryResolver)
        with pytest.raises(CompilationError, match="must be a string"):
            resolver(5)

    def test_not_available_in_tests(self):
        resolver = make_resolver(ParseSavedQueryResolver, make_model(NodeType.Test))
        with pytest.raises(CompilationError, match="only available in models and analyses"):
            resolver("revenue_by_region")


class TestRuntimeResolvers:
    def test_semantic_query_can_not_yet_be_compiled(self):
        resolver = make_resolver(RuntimeMetricQueryResolver)
        with pytest.raises(CompilationError, match="can not yet be compiled"):
            resolver(metrics="revenue")

    def test_saved_query_can_not_yet_be_compiled(self):
        resolver = make_resolver(RuntimeSavedQueryResolver)
        with pytest.raises(CompilationError, match="can not yet be compiled"):
            resolver("revenue_by_region")
