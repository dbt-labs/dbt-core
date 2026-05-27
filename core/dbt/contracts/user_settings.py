from dataclasses import dataclass, field
from typing import Any, Dict

from dbt_common.dataclass_schema import dbtClassMixin


@dataclass
class UserSettings(dbtClassMixin):
    flags: Dict[str, Any] = field(default_factory=dict)
