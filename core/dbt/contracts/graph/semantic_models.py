from dataclasses import dataclass
from dbt.dataclass_schema import dbtClassMixin
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    MeasureReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    TimeGranularity,
)
from typing import List, Optional


@dataclass
class FileSlice(dbtClassMixin):
    """Provides file slice level context about what something was created from.

    Implementation of the dbt-semantic-interfaces `FileSlice` protocol
    """

    filename: str
    content: str
    start_line_number: int
    end_line_number: int


@dataclass
class SourceFileMetadata(dbtClassMixin):
    """Provides file context about what something was created from.

    Implementation of the dbt-semantic-interfaces `Metadata` protocol
    """

    repo_file_path: str
    file_slice: FileSlice


@dataclass
class Defaults(dbtClassMixin):
    agg_time_dimension: Optional[str] = None


# ====================================
# Dimension objects
# ====================================


@dataclass
class DimensionValidityParams(dbtClassMixin):
    is_start: bool = False
    is_end: bool = False


@dataclass
class DimensionTypeParams(dbtClassMixin):
    time_granularity: TimeGranularity
    validity_params: Optional[DimensionValidityParams] = None


@dataclass
class Dimension(dbtClassMixin):
    name: str
    type: DimensionType
    description: Optional[str] = None
    label: Optional[str] = None
    is_partition: bool = False
    type_params: Optional[DimensionTypeParams] = None
    expr: Optional[str] = None
    metadata: Optional[SourceFileMetadata] = None

    @property
    def reference(self) -> DimensionReference:
        return DimensionReference(element_name=self.name)

    @property
    def time_dimension_reference(self) -> Optional[TimeDimensionReference]:
        if self.type == DimensionType.TIME:
            return TimeDimensionReference(element_name=self.name)
        else:
            return None

    @property
    def validity_params(self) -> Optional[DimensionValidityParams]:
        if self.type_params:
            return self.type_params.validity_params
        else:
            return None


# ====================================
# Entity objects
# ====================================


@dataclass
class Entity(dbtClassMixin):
    name: str
    type: EntityType
    description: Optional[str] = None
    label: Optional[str] = None
    role: Optional[str] = None
    expr: Optional[str] = None

    @property
    def reference(self) -> EntityReference:
        return EntityReference(element_name=self.name)

    @property
    def is_linkable_entity_type(self) -> bool:
        return self.type in (EntityType.PRIMARY, EntityType.UNIQUE, EntityType.NATURAL)


# ====================================
# Measure objects
# ====================================


@dataclass
class MeasureAggregationParameters(dbtClassMixin):
    percentile: Optional[float] = None
    use_discrete_percentile: bool = False
    use_approximate_percentile: bool = False


@dataclass
class NonAdditiveDimension(dbtClassMixin):
    name: str
    window_choice: AggregationType
    window_groupings: List[str]


@dataclass
class Measure(dbtClassMixin):
    name: str
    agg: AggregationType
    description: Optional[str] = None
    label: Optional[str] = None
    create_metric: bool = False
    expr: Optional[str] = None
    agg_params: Optional[MeasureAggregationParameters] = None
    non_additive_dimension: Optional[NonAdditiveDimension] = None
    agg_time_dimension: Optional[str] = None

    @property
    def reference(self) -> MeasureReference:
        return MeasureReference(element_name=self.name)
