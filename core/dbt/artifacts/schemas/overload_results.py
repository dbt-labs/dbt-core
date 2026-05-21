from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from dbt_common.dataclass_schema import dbtClassMixin


@dataclass
class OverloadResults(dbtClassMixin):
    successful: List[str] = field(default_factory=list)
    failed: List[str] = field(default_factory=list)

    def __add__(self, other: OverloadResults) -> OverloadResults:
        return OverloadResults(
            successful=self.successful + other.successful,
            failed=self.failed + other.failed,
        )

    def __len__(self):
        return len(self.successful) + len(self.failed)
