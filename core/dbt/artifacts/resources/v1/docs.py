from dataclasses import dataclass
from dbt_common.dataclass_schema import dbtClassMixin
from typing import Optional


@dataclass
class Docs(dbtClassMixin):
    show: bool = True
    node_color: Optional[str] = None
