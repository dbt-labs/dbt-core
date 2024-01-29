from dataclasses import dataclass
from dbt.artifacts.resources.v1.semantic_layer_components import WhereFilterIntersection
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from dbt_semantic_interfaces.type_enums import ConversionCalculationType, TimeGranularity
from typing import List, Optional


@dataclass
class MetricInputMeasure(dbtClassMixin):
    name: str
    filter: Optional[WhereFilterIntersection] = None
    alias: Optional[str] = None
    join_to_timespine: bool = False
    fill_nulls_with: Optional[int] = None

    def measure_reference(self) -> MeasureReference:
        return MeasureReference(element_name=self.name)

    def post_aggregation_measure_reference(self) -> MeasureReference:
        return MeasureReference(element_name=self.alias or self.name)


@dataclass
class MetricTimeWindow(dbtClassMixin):
    count: int
    granularity: TimeGranularity


@dataclass
class MetricInput(dbtClassMixin):
    name: str
    filter: Optional[WhereFilterIntersection] = None
    alias: Optional[str] = None
    offset_window: Optional[MetricTimeWindow] = None
    offset_to_grain: Optional[TimeGranularity] = None

    def as_reference(self) -> MetricReference:
        return MetricReference(element_name=self.name)

    def post_aggregation_reference(self) -> MetricReference:
        return MetricReference(element_name=self.alias or self.name)


@dataclass
class ConstantPropertyInput(dbtClassMixin):
    base_property: str
    conversion_property: str


@dataclass
class ConversionTypeParams(dbtClassMixin):
    base_measure: MetricInputMeasure
    conversion_measure: MetricInputMeasure
    entity: str
    calculation: ConversionCalculationType = ConversionCalculationType.CONVERSION_RATE
    window: Optional[MetricTimeWindow] = None
    constant_properties: Optional[List[ConstantPropertyInput]] = None
