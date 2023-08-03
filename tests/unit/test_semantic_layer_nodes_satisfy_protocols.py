from dbt.contracts.graph.nodes import (
    Metric,
    MetricInput,
    MetricInputMeasure,
    MetricTypeParams,
    NodeRelation,
    SemanticModel,
    WhereFilter,
)
from dbt.contracts.graph.semantic_models import (
    Dimension,
    DimensionTypeParams,
    Entity,
    Measure,
    NonAdditiveDimension,
)
from dbt.node_types import NodeType
from dbt_semantic_interfaces.protocols import (
    Dimension as DSIDimension,
    Entity as DSIEntitiy,
    Measure as DSIMeasure,
    Metric as DSIMetric,
    MetricInput as DSIMetricInput,
    MetricInputMeasure as DSIMetricInputMeasure,
    MetricTypeParams as DSIMetricTypeParams,
    SemanticModel as DSISemanticModel,
    WhereFilter as DSIWhereFilter,
)
from dbt_semantic_interfaces.protocols.measure import (
    NonAdditiveDimensionParameters as DSINonAdditiveDimensionParameters,
)
from dbt_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
    TimeGranularity,
)
from typing import Protocol, runtime_checkable


@runtime_checkable
class RuntimeCheckableSemanticModel(DSISemanticModel, Protocol):
    pass


@runtime_checkable
class RuntimeCheckableDimension(DSIDimension, Protocol):
    pass


@runtime_checkable
class RuntimeCheckableEntity(DSIEntitiy, Protocol):
    pass


@runtime_checkable
class RuntimeCheckableMeasure(DSIMeasure, Protocol):
    pass


@runtime_checkable
class RuntimeCheckableMetric(DSIMetric, Protocol):
    pass


@runtime_checkable
class RuntimeCheckableMetricInput(DSIMetricInput, Protocol):
    pass


@runtime_checkable
class RuntimeCheckableMetricInputMeasure(DSIMetricInputMeasure, Protocol):
    pass


@runtime_checkable
class RuntimeCheckableMetricTypeParams(DSIMetricTypeParams, Protocol):
    pass


@runtime_checkable
class RuntimeCheckableWhereFilter(DSIWhereFilter, Protocol):
    pass


@runtime_checkable
class RuntimeCheckableNonAdditiveDimension(DSINonAdditiveDimensionParameters, Protocol):
    pass


def test_semantic_model_node_satisfies_protocol():
    test_semantic_model = SemanticModel(
        name="test_semantic_model",
        description="a test semantic_model",
        resource_type=NodeType.SemanticModel,
        package_name="package_name",
        path="path.to.semantic_model",
        original_file_path="path/to/file",
        unique_id="not_like_the_other_semantic_models",
        fqn=["fully", "qualified", "name"],
        model="ref('a_model')",
        node_relation=NodeRelation(
            alias="test_alias",
            schema_name="test_schema_name",
        ),
        entities=[],
        measures=[],
        dimensions=[],
    )
    assert isinstance(test_semantic_model, RuntimeCheckableSemanticModel)


def test_dimension_satisfies_protocol():
    dimension = Dimension(
        name="test_dimension",
        description="a test dimension",
        type=DimensionType.TIME,
        type_params=DimensionTypeParams(
            time_granularity=TimeGranularity.DAY,
        ),
    )
    assert isinstance(dimension, RuntimeCheckableDimension)


def test_entity_satisfies_protocol():
    entity = Entity(
        name="test_entity",
        description="a test entity",
        type=EntityType.PRIMARY,
        expr="id",
        role="a_role",
    )
    assert isinstance(entity, RuntimeCheckableEntity)


def test_measure_satisfies_protocol():
    measure = Measure(
        name="test_measure",
        description="a test measure",
        agg="sum",
        create_metric=True,
        expr="amount",
        agg_time_dimension="a_time_dimension",
    )
    assert isinstance(measure, RuntimeCheckableMeasure)


def test_metric_node_satisfies_protocol():
    metric = Metric(
        name="a_metric",
        resource_type=NodeType.Metric,
        package_name="package_name",
        path="path.to.semantic_model",
        original_file_path="path/to/file",
        unique_id="not_like_the_other_semantic_models",
        fqn=["fully", "qualified", "name"],
        description="a test metric",
        label="A test metric",
        type=MetricType.SIMPLE,
        type_params=MetricTypeParams(
            measure=MetricInputMeasure(
                name="a_test_measure", filter=WhereFilter(where_sql_template="a_dimension is true")
            )
        ),
    )
    assert isinstance(metric, RuntimeCheckableMetric)


def test_where_filter_satisfies_protocol():
    where_filter = WhereFilter(
        where_sql_template="{{ Dimension('enity_name__dimension_name') }} AND {{ TimeDimension('entity_name__time_dimension_name', 'month') }} AND {{ Entity('entity_name') }}"
    )
    assert isinstance(where_filter, RuntimeCheckableWhereFilter)


def test_metric_input():
    metric_input = MetricInput(name="a_metric_input")
    assert isinstance(metric_input, RuntimeCheckableMetricInput)


def test_metric_input_measure():
    metric_input_measure = MetricInputMeasure(name="a_metric_input_measure")
    assert isinstance(metric_input_measure, RuntimeCheckableMetricInputMeasure)


def test_metric_type_params_satisfies_protocol():
    type_params = MetricTypeParams()
    assert isinstance(type_params, RuntimeCheckableMetricTypeParams)


def test_non_additive_dimension_satisfies_protocol():
    non_additive_dimension = NonAdditiveDimension(
        name="dimension_name",
        window_choice=AggregationType.MIN,
        window_groupings=["entity_name"],
    )
    assert isinstance(non_additive_dimension, RuntimeCheckableNonAdditiveDimension)
