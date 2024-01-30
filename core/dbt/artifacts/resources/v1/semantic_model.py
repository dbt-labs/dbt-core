from dataclasses import dataclass, field
from dbt_common.contracts.config.base import BaseConfig, CompareBehavior, MergeBehavior
from dbt_common.dataclass_schema import dbtClassMixin
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
from dbt.artifacts.resources import SourceFileMetadata
from typing import Any, Dict, List, Optional


@dataclass
class Defaults(dbtClassMixin):
    agg_time_dimension: Optional[str] = None


@dataclass
class NodeRelation(dbtClassMixin):
    alias: str
    schema_name: str  # TODO: Could this be called simply "schema" so we could reuse StateRelation?
    database: Optional[str] = None
    relation_name: Optional[str] = None


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


# ====================================
# SemanticModel final parts
# ====================================


@dataclass
class SemanticModelConfig(BaseConfig):
    enabled: bool = True
    group: Optional[str] = field(
        default=None,
        metadata=CompareBehavior.Exclude.meta(),
    )
    meta: Dict[str, Any] = field(
        default_factory=dict,
        metadata=MergeBehavior.Update.meta(),
    )
