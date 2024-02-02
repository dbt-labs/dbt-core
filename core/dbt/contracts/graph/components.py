from dataclasses import dataclass
from datetime import timedelta
from dbt.artifacts.resources import Time as TimeResource
from typing import Dict


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
