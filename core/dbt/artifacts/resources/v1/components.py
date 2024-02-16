from dataclasses import dataclass, field
from datetime import timedelta
from dbt.artifacts.resources.types import TimePeriod
from dbt.artifacts.resources.v1.macro import MacroDependsOn
from dbt_common.contracts.config.properties import AdditionalPropertiesMixin
from dbt_common.contracts.constraints import ColumnLevelConstraint
from dbt_common.contracts.util import Mergeable
from dbt_common.dataclass_schema import dbtClassMixin, ExtensibleDbtClassMixin
from typing import Any, Dict, List, Optional, Union


NodeVersion = Union[str, float]


@dataclass
class DependsOn(MacroDependsOn):
    nodes: List[str] = field(default_factory=list)

    def add_node(self, value: str):
        if value not in self.nodes:
            self.nodes.append(value)


@dataclass
class RefArgs(dbtClassMixin):
    name: str
    package: Optional[str] = None
    version: Optional[NodeVersion] = None

    @property
    def positional_args(self) -> List[str]:
        if self.package:
            return [self.package, self.name]
        else:
            return [self.name]

    @property
    def keyword_args(self) -> Dict[str, Optional[NodeVersion]]:
        if self.version:
            return {"version": self.version}
        else:
            return {}


@dataclass
class ColumnInfo(AdditionalPropertiesMixin, ExtensibleDbtClassMixin):
    """Used in all ManifestNodes and SourceDefinition"""

    name: str
    description: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    data_type: Optional[str] = None
    constraints: List[ColumnLevelConstraint] = field(default_factory=list)
    quote: Optional[bool] = None
    tags: List[str] = field(default_factory=list)
    _extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Quoting(dbtClassMixin, Mergeable):
    database: Optional[bool] = None
    schema: Optional[bool] = None
    identifier: Optional[bool] = None
    column: Optional[bool] = None


@dataclass
class Time(dbtClassMixin, Mergeable):
    count: Optional[int] = None
    period: Optional[TimePeriod] = None

    def exceeded(self, actual_age: float) -> bool:
        if self.period is None or self.count is None:
            return False
        kwargs: Dict[str, int] = {self.period.plural(): self.count}
        difference = timedelta(**kwargs).total_seconds()
        return actual_age > difference

    def __bool__(self):
        return self.count is not None and self.period is not None


@dataclass
class FreshnessThreshold(dbtClassMixin, Mergeable):
    warn_after: Optional[Time] = field(default_factory=Time)
    error_after: Optional[Time] = field(default_factory=Time)
    filter: Optional[str] = None

    def status(self, age: float) -> "dbt.artifacts.schemas.results.FreshnessStatus":  # type: ignore # noqa F821
        from dbt.artifacts.schemas.results import FreshnessStatus

        if self.error_after and self.error_after.exceeded(age):
            return FreshnessStatus.Error
        elif self.warn_after and self.warn_after.exceeded(age):
            return FreshnessStatus.Warn
        else:
            return FreshnessStatus.Pass

    def __bool__(self):
        return bool(self.warn_after) or bool(self.error_after)


@dataclass
class HasRelationMetadata(dbtClassMixin):
    database: Optional[str]
    schema: str

    # Can't set database to None like it ought to be
    # because it messes up the subclasses and default parameters
    # so hack it here
    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        if "database" not in data:
            data["database"] = None
        return data

    @property
    def quoting_dict(self) -> Dict[str, bool]:
        if hasattr(self, "quoting"):
            return self.quoting.to_dict(omit_none=True)
        else:
            return {}
