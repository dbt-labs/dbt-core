from dataclasses import dataclass, field
from datetime import timedelta
from dbt.artifacts.resources import (
    FreshnessThreshold as FreshnessThresholdResource,
    Time as TimeResource,
)
from typing import Dict, Optional


@dataclass
class Time(TimeResource):
    def exceeded(self, actual_age: float) -> bool:
        if self.period is None or self.count is None:
            return False
        kwargs: Dict[str, int] = {self.period.plural(): self.count}
        difference = timedelta(**kwargs).total_seconds()
        return actual_age > difference

    def __bool__(self):
        return self.count is not None and self.period is not None


@dataclass
class FreshnessThreshold(FreshnessThresholdResource):
    # Overriding the FreshnessThresholdResource.warn_after to use Time instead of TimeResource
    warn_after: Optional[Time] = field(default_factory=Time)
    # Overriding the FreshnessThresholdResource.error_after to use Time instead of TimeResource
    error_after: Optional[Time] = field(default_factory=Time)

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
