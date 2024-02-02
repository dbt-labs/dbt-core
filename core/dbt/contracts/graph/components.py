from dataclasses import dataclass
from datetime import timedelta
from dbt.artifacts.resources.types import TimePeriod
from dbt_common.contracts.util import Mergeable
from dbt_common.dataclass_schema import dbtClassMixin
from typing import Dict, Optional


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
