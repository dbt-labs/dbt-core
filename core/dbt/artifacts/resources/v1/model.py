import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Literal, Optional

from dbt.artifacts.resources.types import AccessType, NodeType, TimePeriod
from dbt.artifacts.resources.v1.components import (
    CompiledResource,
    DeferRelation,
    NodeVersion,
)
from dbt.artifacts.resources.v1.config import NodeConfig
from dbt_common.contracts.config.base import MergeBehavior
from dbt_common.contracts.constraints import ModelLevelConstraint
from dbt_common.contracts.util import Mergeable
from dbt_common.dataclass_schema import (
    ExtensibleDbtClassMixin,
    ValidationError,
    dbtClassMixin,
)


class ModelFreshnessUpdatesOnOptions(enum.Enum):
    all = "all"
    any = "any"


@dataclass
class ModelBuildAfter(ExtensibleDbtClassMixin):
    count: Optional[int] = None
    period: Optional[TimePeriod] = None
    updates_on: ModelFreshnessUpdatesOnOptions = ModelFreshnessUpdatesOnOptions.any


@dataclass
class ModelFreshness(ExtensibleDbtClassMixin, Mergeable):
    build_after: ModelBuildAfter


def merge_model_freshness(*thresholds: Optional[ModelFreshness]) -> Optional[ModelFreshness]:
    if not thresholds:
        return None

    current_merged_value: Optional[ModelFreshness] = thresholds[0]

    for i in range(1, len(thresholds)):
        base = current_merged_value
        update = thresholds[i]

        if base is not None and update is not None:
            # When both base and update freshness are defined,
            # create a new ModelFreshness instance using the build_after from the 'update'.
            # This effectively means 'update's build_after configuration takes precedence.
            merged_freshness_obj = base.merged(update)
            if (
                base.build_after.updates_on == ModelFreshnessUpdatesOnOptions.all
                or update.build_after.updates_on == ModelFreshnessUpdatesOnOptions.all
            ):
                merged_freshness_obj.build_after.updates_on = ModelFreshnessUpdatesOnOptions.all
            current_merged_value = merged_freshness_obj
        elif base is None and update is not None:
            # If the current merged value is None but the new update is defined,
            # take the update.
            current_merged_value = update
        else:
            # This covers cases where 'update' is None (regardless of 'base'),
            # or both 'base' and 'update' are None.
            # The result of the pair-merge is None.
            current_merged_value = base

    return current_merged_value


@dataclass
class ModelConfig(NodeConfig):
    access: AccessType = field(
        default=AccessType.Protected,
        metadata=MergeBehavior.Clobber.meta(),
    )
    freshness: Optional[ModelFreshness] = None

    def __post_init__(self):
        super().__post_init__()
        if (
            self.freshness
            and self.freshness.build_after.period
            and self.freshness.build_after.count is None
        ):
            raise ValidationError(
                "`freshness.build_after` must have a value for `count` if a `period` is provided"
            )
        elif (
            self.freshness
            and self.freshness.build_after.count is not None
            and not self.freshness.build_after.period
        ):
            raise ValidationError(
                "`freshness.build_after` must have a value for `period` if a `count` is provided"
            )

    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        # scrub out model configs where "build_after" is not defined
        if (
            "freshness" in data
            and isinstance(data["freshness"], dict)
            and "build_after" in data["freshness"]
        ):
            data["freshness"] = ModelFreshness.from_dict(data["freshness"]).to_dict()
        else:
            data.pop("freshness", None)
        return data


@dataclass
class CustomGranularity(dbtClassMixin):
    name: str
    column_name: Optional[str] = None


@dataclass
class TimeSpine(dbtClassMixin):
    standard_granularity_column: str
    custom_granularities: List[CustomGranularity] = field(default_factory=list)


@dataclass
class Model(CompiledResource):
    resource_type: Literal[NodeType.Model]
    access: AccessType = AccessType.Protected
    config: ModelConfig = field(default_factory=ModelConfig)
    constraints: List[ModelLevelConstraint] = field(default_factory=list)
    version: Optional[NodeVersion] = None
    latest_version: Optional[NodeVersion] = None
    deprecation_date: Optional[datetime] = None
    defer_relation: Optional[DeferRelation] = None
    primary_key: List[str] = field(default_factory=list)
    time_spine: Optional[TimeSpine] = None

    def __post_serialize__(self, dct: Dict, context: Optional[Dict] = None):
        dct = super().__post_serialize__(dct, context)
        if context and context.get("artifact") and "defer_relation" in dct:
            del dct["defer_relation"]
        return dct
