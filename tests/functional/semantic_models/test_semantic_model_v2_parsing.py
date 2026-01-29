import pytest

from dbt.contracts.graph.manifest import Manifest
from dbt_semantic_interfaces.type_enums import (
    AggregationType,
    ConversionCalculationType,
    DimensionType,
    EntityType,
    MetricType,
    PeriodAggregation,
)
from tests.functional.assertions.test_runner import dbtTestRunner
from tests.functional.semantic_models.fixtures import (
    base_schema_yml_v2,
    derived_semantics_yml,
    fct_revenue_sql,
    metricflow_time_spine_sql,
    schema_yml_v2_conversion_metric_missing_base_metric,
    schema_yml_v2_cumulative_metric_missing_input_metric,
    schema_yml_v2_simple_metric_on_model_1,
    schema_yml_v2_standalone_simple_metric,
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
        assert len(manifest.metrics) == 0
        # TODO: Dimensions are not parsed yet (for those attached to model columns)
        # TODO: Dimensions are not parsed yet (for those defined in derived semantics)


class TestStandaloneMetricParsingWorks:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": base_schema_yml_v2
            + schema_yml_v2_simple_metric_on_model_1,  # schema_yml_v2_standalone_metrics,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_included_metric_parsing(self, project):
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success
        manifest = result.result
        metrics = manifest.metrics
        assert len(metrics) == 5

        simple_metric = metrics["metric.test.simple_metric"]
        assert simple_metric.name == "simple_metric"
        assert simple_metric.description == "This is our first simple metric."
        assert simple_metric.type == MetricType.SIMPLE
        assert simple_metric.type_params.metric_aggregation_params.agg == AggregationType.COUNT
        assert simple_metric.type_params.metric_aggregation_params.semantic_model == "fct_revenue"
        assert "semantic_model.test.fct_revenue" in simple_metric.depends_on.nodes

        simple_metric_2 = metrics["metric.test.simple_metric_2"]
        assert simple_metric_2.name == "simple_metric_2"
        assert simple_metric_2.description == "This is our second simple metric."
        assert simple_metric_2.type == MetricType.SIMPLE
        assert simple_metric_2.type_params.metric_aggregation_params.agg == AggregationType.COUNT
        assert (
            simple_metric_2.type_params.metric_aggregation_params.semantic_model == "fct_revenue"
        )
        assert "semantic_model.test.fct_revenue" in simple_metric_2.depends_on.nodes

        percentile_metric = metrics["metric.test.percentile_metric"]
        assert percentile_metric.name == "percentile_metric"
        assert percentile_metric.description == "This is our percentile metric."
        assert percentile_metric.type == MetricType.SIMPLE
        assert (
            percentile_metric.type_params.metric_aggregation_params.agg
            == AggregationType.PERCENTILE
        )
        assert (
            percentile_metric.type_params.metric_aggregation_params.semantic_model == "fct_revenue"
        )
        assert (
            percentile_metric.type_params.metric_aggregation_params.agg_params.percentile == 0.99
        )
        assert (
            percentile_metric.type_params.metric_aggregation_params.agg_params.use_discrete_percentile
            is True
        )
        assert (
            percentile_metric.type_params.metric_aggregation_params.agg_params.use_approximate_percentile
            is False
        )
        assert "semantic_model.test.fct_revenue" in percentile_metric.depends_on.nodes

        cumulative_metric = metrics["metric.test.cumulative_metric"]
        assert cumulative_metric.name == "cumulative_metric"
        assert cumulative_metric.description == "This is our cumulative metric."
        assert cumulative_metric.type == MetricType.CUMULATIVE
        assert cumulative_metric.type_params.cumulative_type_params.grain_to_date == "day"
        assert (
            cumulative_metric.type_params.cumulative_type_params.period_agg
            == PeriodAggregation.FIRST
        )
        assert cumulative_metric.type_params.cumulative_type_params.metric.name == "simple_metric"
        assert "metric.test.simple_metric" in cumulative_metric.depends_on.nodes

        conversion_metric = metrics["metric.test.conversion_metric"]
        assert conversion_metric.name == "conversion_metric"
        assert conversion_metric.description == "This is our conversion metric."
        assert conversion_metric.type == MetricType.CONVERSION
        assert conversion_metric.type_params.conversion_type_params.entity == "id_entity"
        assert (
            conversion_metric.type_params.conversion_type_params.calculation
            is ConversionCalculationType.CONVERSION_RATE
        )
        assert (
            conversion_metric.type_params.conversion_type_params.base_metric.name
            == "simple_metric"
        )
        assert (
            conversion_metric.type_params.conversion_type_params.conversion_metric.name
            == "simple_metric_2"
        )
        assert "metric.test.simple_metric" in conversion_metric.depends_on.nodes
        assert "metric.test.simple_metric_2" in conversion_metric.depends_on.nodes


class TestStandaloneMetricParsingSimpleMetricFails:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": base_schema_yml_v2 + schema_yml_v2_standalone_simple_metric,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_standalone_metric_parsing(self, project):
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert not result.success
        assert (
            "simple metrics in v2 YAML must be attached to semantic_model" in result.exception.msg
        )


class TestCumulativeMetricNoInputMetricFails:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": base_schema_yml_v2
            + schema_yml_v2_cumulative_metric_missing_input_metric,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_cumulative_metric_no_input_metric_parsing_fails(self, project) -> None:
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert not result.success
        assert "input_metric is required for cumulative metrics." in str(result.exception)


class TestConversionMetricNoBaseMetricFails:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": base_schema_yml_v2 + schema_yml_v2_conversion_metric_missing_base_metric,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_conversion_metric_no_base_metric_parsing_fails(self, project) -> None:
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert not result.success
        assert "base_metric is required for conversion metrics." in str(result.exception)


class TestDerivedSemanticsParsingWorks:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": semantic_model_schema_yml_v2 + derived_semantics_yml,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_derived_semantics_parsing(self, project) -> None:
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        assert result.success
        manifest = result.result
        assert len(manifest.semantic_models) == 1
        semantic_model = manifest.semantic_models["semantic_model.test.fct_revenue"]
        entities = {entity.name: entity for entity in semantic_model.entities}
        assert (
            len(entities) == 5
        )  # length is so long because it is column entities + derived entities

        id_entity = entities["derived_id_entity"]
        assert id_entity.type == EntityType.PRIMARY
        assert id_entity.description == "This is the id entity, and it is the primary entity."
        assert id_entity.expr == "id + foreign_id_col"
        assert id_entity.config.meta == {"test_label_thing": "derived_entity_1"}

        id_entity_with_no_optional_fields = entities["derived_id_entity_with_no_optional_fields"]
        assert id_entity_with_no_optional_fields.type == EntityType.PRIMARY
        assert id_entity_with_no_optional_fields.expr == "id + foreign_id_col"
        assert id_entity_with_no_optional_fields.config.meta == {}


# TODO DI-4605: add enforcement and a test for when there are validity params with no column granularity
# TODO DI-4603: add enforcement and a test for a TIME type dimension and a column that has no granularity set
