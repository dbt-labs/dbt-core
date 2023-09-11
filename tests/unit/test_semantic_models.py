import pytest

from typing import List

from dbt.contracts.graph.nodes import SemanticModel
from dbt.contracts.graph.semantic_models import Dimension, Entity, Measure, Defaults
from dbt.node_types import NodeType
from dbt_semantic_interfaces.references import MeasureReference
from dbt_semantic_interfaces.type_enums import AggregationType, DimensionType, EntityType


@pytest.fixture(scope="function")
def dimensions() -> List[Dimension]:
    return [Dimension(name="ds", type=DimensionType)]


@pytest.fixture(scope="function")
def entities() -> List[Entity]:
    return [Entity(name="test_entity", type=EntityType.PRIMARY, expr="id")]


@pytest.fixture(scope="function")
def measures() -> List[Measure]:
    return [Measure(name="test_measure", agg=AggregationType.COUNT, expr="id")]


@pytest.fixture(scope="function")
def default_semantic_model(
    dimensions: List[Dimension], entities: List[Entity], measures: List[Measure]
) -> SemanticModel:
    return SemanticModel(
        name="test_semantic_model",
        resource_type=NodeType.SemanticModel,
        model="ref('test_model')",
        package_name="test",
        path="test_path",
        original_file_path="test_fixture",
        unique_id=f"{NodeType.SemanticModel}.test.test_semantic_model",
        fqn=[],
        defaults=Defaults(agg_time_dimension="ds"),
        dimensions=dimensions,
        entities=entities,
        measures=measures,
        node_relation=None,
    )


def test_checked_agg_time_dimension_for_measure_via_defaults(
    default_semantic_model: SemanticModel,
):
    assert default_semantic_model.defaults.agg_time_dimension is not None
    measure = default_semantic_model.measures[0]
    measure.agg_time_dimension = None
    default_semantic_model.checked_agg_time_dimension_for_measure(
        MeasureReference(element_name=measure.name)
    )


def test_checked_agg_time_dimension_for_measure_via_measure(default_semantic_model: SemanticModel):
    default_semantic_model.defaults = None
    measure = default_semantic_model.measures[0]
    measure.agg_time_dimension = default_semantic_model.dimensions[0].name
    default_semantic_model.checked_agg_time_dimension_for_measure(
        MeasureReference(element_name=measure.name)
    )


def test_checked_agg_time_dimension_for_measure_exception(default_semantic_model: SemanticModel):
    default_semantic_model.defaults = None
    measure = default_semantic_model.measures[0]
    measure.agg_time_dimension = None

    with pytest.raises(AssertionError) as execinfo:
        default_semantic_model.checked_agg_time_dimension_for_measure(
            MeasureReference(measure.name)
        )

    assert (
        f"Aggregation time dimension for measure {measure.name} on semantic model {default_semantic_model.name}"
        in str(execinfo.value)
    )
