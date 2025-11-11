import pytest

from dbt.contracts.graph.manifest import Manifest
from dbt_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
)
from tests.functional.assertions.test_runner import dbtTestRunner
from tests.functional.semantic_models.fixtures import (
    base_schema_yml_v2,
    fct_revenue_sql,
    metricflow_time_spine_sql,
    schema_yml_v2_metrics,
    semantic_model_schema_yml_v2,
)


class TestSemanticModelParsingWorks:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": semantic_model_schema_yml_v2,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_semantic_model_parsing(self, project) -> None:
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success
        assert isinstance(result.result, Manifest)
        manifest = result.result
        assert len(manifest.semantic_models) == 1
        # TODO: Add support for renaming semantic model to be different than the dbt model
        # semantic_model = manifest.semantic_models["semantic_model.test.revenue"]
        semantic_model = manifest.semantic_models["semantic_model.test.fct_revenue"]
        assert semantic_model.node_relation.alias == "fct_revenue"
        assert (
            semantic_model.node_relation.relation_name
            == f'"dbt"."{project.test_schema}"."fct_revenue"'
        )
        assert (
            semantic_model.description
            == "This is the model fct_revenue. It should be able to use doc blocks"
        )

        # Dimensions

        assert len(semantic_model.dimensions) == 3
        dimensions = {dimension.name: dimension for dimension in semantic_model.dimensions}
        id_dim = dimensions["id_dim"]
        assert id_dim.type == DimensionType.CATEGORICAL
        assert id_dim.description == "This is the id column dim."
        assert id_dim.label == "ID Dimension"
        assert id_dim.is_partition is True
        assert id_dim.config.meta == {"component_level": "dimension_override"}
        second_dim = dimensions["second_dim"]
        assert second_dim.type == DimensionType.TIME
        assert second_dim.description == "This is the second column (dim)."
        assert second_dim.label == "Second Dimension"
        assert second_dim.is_partition is False
        assert second_dim.config.meta == {}
        assert second_dim.type_params.validity_params.is_start is True
        assert second_dim.type_params.validity_params.is_end is True
        col_with_default_dimensions = dimensions["col_with_default_dimensions"]
        assert col_with_default_dimensions.type == DimensionType.CATEGORICAL
        assert (
            col_with_default_dimensions.description
            == "This is the column with default dimension settings."
        )
        assert col_with_default_dimensions.label is None
        assert col_with_default_dimensions.is_partition is False
        assert col_with_default_dimensions.config.meta == {}
        assert col_with_default_dimensions.validity_params is None

        # Entities
        assert len(semantic_model.entities) == 3
        entities = {entity.name: entity for entity in semantic_model.entities}
        primary_entity = entities["id_entity"]
        assert primary_entity.type == EntityType.PRIMARY
        assert primary_entity.description == "This is the id entity, and it is the primary entity."
        assert primary_entity.label == "ID Entity"
        assert primary_entity.config.meta == {"component_level": "entity_override"}

        foreign_id_col = entities["foreign_id_col"]
        assert foreign_id_col.type == EntityType.FOREIGN
        assert foreign_id_col.description == "This is a foreign id column."
        assert foreign_id_col.label is None
        assert foreign_id_col.config.meta == {}

        col_with_default_entity_testing_default_desc = entities[
            "col_with_default_entity_testing_default_desc"
        ]
        assert col_with_default_entity_testing_default_desc.type == EntityType.NATURAL
        assert (
            col_with_default_entity_testing_default_desc.description
            == "This is the column with default dimension settings."
        )
        assert col_with_default_entity_testing_default_desc.label is None
        assert col_with_default_entity_testing_default_desc.config.meta == {}

        # No measures in v2 YAML
        assert len(semantic_model.measures) == 0
        # TODO: Metrics are not parsed yet
        assert len(manifest.metrics) == 0
        # TODO: Dimensions are not parsed yet (for those attached to model columns)
        # TODO: Dimensions are not parsed yet (for those defined in derived semantics)


class TestStandaloneMetricParsingWorks:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": base_schema_yml_v2 + schema_yml_v2_metrics,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_standalone_metric_parsing(self, project):
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success
        manifest = result.result
        metrics = manifest.metrics
        assert len(metrics) == 1
        metric = metrics["metric.test.simple_metric"]
        assert metric.name == "simple_metric"
        assert metric.description == "This is our first simple metric."
        assert metric.type == MetricType.SIMPLE
        assert metric.type_params.metric_aggregation_params.agg == AggregationType.COUNT


# TODO DI-4605: add enforcement and a test for when there are validity params with no column granularity
# TODO DI-4603: add enforcement and a test for a TIME type dimension and a column that has no granularity set
