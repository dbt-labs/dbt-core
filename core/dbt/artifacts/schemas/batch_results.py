from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Tuple

from dbt_common.dataclass_schema import dbtClassMixin

BatchType = Tuple[datetime, datetime]


@dataclass
class BatchResults(dbtClassMixin):
    successful: List[BatchType] = field(default_factory=list)
    failed: List[BatchType] = field(default_factory=list)

    def __add__(self, other: BatchResults) -> BatchResults:
        return BatchResults(
            successful=self.successful + other.successful,
            failed=self.failed + other.failed,
        )

    def __len__(self):
        return len(self.successful) + len(self.failed)
