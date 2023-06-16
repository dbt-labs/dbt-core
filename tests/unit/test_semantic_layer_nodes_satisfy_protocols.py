from dbt.contracts.graph.nodes import (
    Metric,
    MetricInputMeasure,
    MetricTypeParams,
    NodeRelation,
    SemanticModel,
    WhereFilter,
)
from dbt.contracts.graph.semantic_models import Dimension, DimensionTypeParams, Entity, Measure
from dbt.node_types import NodeType
from dbt_semantic_interfaces.protocols.dimension import Dimension as DSIDimension
from dbt_semantic_interfaces.protocols.entity import Entity as DSIEntitiy
from dbt_semantic_interfaces.protocols.measure import Measure as DSIMeasure
from dbt_semantic_interfaces.protocols.metric import Metric as DSIMetric
from dbt_semantic_interfaces.protocols.semantic_model import SemanticModel as DSISemanticModel
from dbt_semantic_interfaces.type_enums.dimension_type import DimensionType
from dbt_semantic_interfaces.type_enums.entity_type import EntityType
from dbt_semantic_interfaces.type_enums.metric_type import MetricType
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
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
